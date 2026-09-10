"""CLI for read-only validation, safe startup reconciliation, and freezing."""

import argparse
import json
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path

from recorder.catalog import DatasetFileManifest
from recorder.durability import DatasetLock, recover_dataset
from recorder.frozen import freeze_dataset, select_manifests, verify_frozen
from replay.reconstruction import validate_raw_reconstruction
from replay.streaming import iter_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, default=Path("data"))
    parser.add_argument("--repair", action="store_true")
    parser.add_argument("--freeze", action="store_true")
    parser.add_argument("--frozen", type=Path)
    parser.add_argument("--reconstruct", action="store_true", help="REST + raw depth equivalence")
    parser.add_argument("--symbol", choices=("BTCUSDT", "BTCUSDC"))
    args = parser.parse_args()
    if args.frozen:
        print(json.dumps(verify_frozen(args.data_directory, args.frozen), indent=2))
        return
    if args.freeze:
        print(
            freeze_dataset(
                args.data_directory, select_manifests(args.data_directory, symbol=args.symbol)
            )
        )
        return
    with DatasetLock(args.data_directory):
        result = recover_dataset(args.data_directory, repair=args.repair)
        if args.reconstruct and result.valid:
            groups: dict[tuple[str, str], list[DatasetFileManifest]] = defaultdict(list)
            for entry in select_manifests(args.data_directory, symbol=args.symbol):
                groups[(entry.capture_session_id, entry.symbol)].append(entry)
            reports = {}
            valid = bool(groups)
            for key, entries in sorted(groups.items()):
                reconstruction = validate_raw_reconstruction(
                    iter_session(args.data_directory, tuple(entries))
                )
                reports["/".join(key)] = asdict(reconstruction)
                valid = valid and reconstruction.valid
            print(json.dumps({"reconstruction": reports, "valid": valid}, indent=2))
            if not valid:
                raise SystemExit(2)
    print(json.dumps(asdict(result), indent=2))
    if not result.valid:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
