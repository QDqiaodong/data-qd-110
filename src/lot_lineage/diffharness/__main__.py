"""Command line entry point: ``python -m lot_lineage.diffharness``.

Four subcommands form the differential workflow::

    generate  --seed N --steps K --output scenario.json
    run       --scenario scenario.json [--prefix P ...] [--counterexample ce.json]
    reduce    --counterexample ce.json --budget B [--prefix P ...] --output reduced.json
    replay    --counterexample ce.json [--prefix P ...]

Everything is deterministic and standard-library only.  The SUT prefix is
configurable so a buggy wrapper can be tested without modifying production
code; the default prefix is the real ``python -m lot_lineage.intake`` CLI.
A run without divergence prints the achieved coverage counts and never
claims the implementation is error-free.
"""
from __future__ import annotations

import argparse
import shlex
import sys

from . import jsonio
from .replay import replay
from .reduce import reduce_counterexample
from .runner import DEFAULT_PREFIX, run_scenario
from .scenario import generate_scenario

EXIT_DIVERGENCE = 4
EXIT_BAD_USAGE = 2


def _split_prefix(text: str | None) -> list[str]:
    if not text:
        return list(DEFAULT_PREFIX)
    return shlex.split(text)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lot_lineage.diffharness",
        description="Differential harness for the weighing-ticket intake CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="deterministically generate a scenario")
    gen.add_argument("--seed", type=int, required=True)
    gen.add_argument("--steps", type=int, required=True)
    gen.add_argument("--output", required=True)

    run = sub.add_parser("run", help="run a scenario against the SUT in real subprocesses")
    run.add_argument("--scenario", required=True)
    run.add_argument("--prefix", default=None,
                     help="shell-style command prefix replacing the default intake CLI")
    run.add_argument("--counterexample", default=None,
                     help="optional path to write the self-contained counterexample")

    red = sub.add_parser("reduce", help="minimise a counterexample on fresh empty libraries")
    red.add_argument("--counterexample", required=True)
    red.add_argument("--budget", type=int, required=True,
                     help="maximum number of fresh-library verification runs")
    red.add_argument("--prefix", default=None)
    red.add_argument("--output", required=True)

    rep = sub.add_parser("replay", help="replay a counterexample without seed/generated files")
    rep.add_argument("--counterexample", required=True)
    rep.add_argument("--prefix", default=None)
    return parser


def _cmd_generate(args) -> int:
    if args.steps < 1:
        raise ValueError("--steps must be a positive integer")
    scenario = generate_scenario(args.seed, args.steps)
    jsonio.write_json(args.output, scenario)
    summary = {
        "written": args.output,
        "seed": args.seed,
        "steps": args.steps,
        "operations": len(scenario["operations"]),
        "files": len(scenario["files"]),
        "coverage_tags": scenario["coverage_tags"],
        "covered_count": len(scenario["coverage_tags"]),
    }
    print(jsonio.dumps(summary))
    return 0


def _cmd_run(args) -> int:
    scenario = jsonio.load_json(args.scenario)
    prefix = _split_prefix(args.prefix)
    outcome = run_scenario(scenario, prefix)
    if outcome["divergent"]:
        if args.counterexample:
            jsonio.write_json(args.counterexample, outcome["counterexample"])
        report = {
            "divergent": True,
            "operations_executed": outcome["operations_executed"],
            "operations_total": outcome["operations_total"],
            "signature": outcome["signature"],
            "counterexample_written": args.counterexample,
        }
        print(jsonio.dumps(report))
        return EXIT_DIVERGENCE

    # No observed difference: report coverage only; never claim "no bugs".
    report = {
        "divergent": False,
        "operations_executed": outcome["operations_executed"],
        "operations_total": outcome["operations_total"],
        "coverage_tags": outcome["coverage_tags"],
        "covered_count": outcome["covered_count"],
        "note": "no divergence observed for this scenario; this is not a "
                "proof that the implementation is error-free",
    }
    print(jsonio.dumps(report))
    return 0


def _cmd_reduce(args) -> int:
    if args.budget < 1:
        raise ValueError("--budget must be a positive integer")
    counterexample = jsonio.load_json(args.counterexample)
    prefix = _split_prefix(args.prefix)
    report = reduce_counterexample(counterexample, prefix, args.budget)
    jsonio.write_json(args.output, report["counterexample"])
    summary = {
        "complete": report["complete"],
        "runs_used": report["runs_used"],
        "budget": report["budget"],
        "operations": report["operations"],
        "csv_records": report["csv_records"],
        "csv_bytes": report["csv_bytes"],
        "signature": report["signature"],
        "reduced_counterexample": args.output,
        "note": (
            "minimality proven on fresh libraries"
            if report["complete"]
            else "budget exhausted before minimality could be proven"
        ),
    }
    print(jsonio.dumps(summary))
    return 0


def _cmd_replay(args) -> int:
    counterexample = jsonio.load_json(args.counterexample)
    prefix = _split_prefix(args.prefix)
    outcome = replay(counterexample, prefix)
    report = {
        "reproduces": outcome["reproduces"],
        "divergent": outcome["divergent"],
        "signature": outcome["signature"],
        "expected_signature": counterexample["signature"],
        "operations_executed": outcome["operations_executed"],
    }
    print(jsonio.dumps(report))
    return EXIT_DIVERGENCE if outcome["reproduces"] else 1


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "generate": _cmd_generate,
        "run": _cmd_run,
        "reduce": _cmd_reduce,
        "replay": _cmd_replay,
    }
    try:
        return handlers[args.command](args)
    except (ValueError, OSError) as exc:
        print(f"diffharness {args.command} failed: {exc}", file=sys.stderr)
        return EXIT_BAD_USAGE


if __name__ == "__main__":
    raise SystemExit(main())
