#!/usr/bin/env python3
import argparse
import json
import random
import time
import uuid
from pathlib import Path


DEFAULT_SIZE_PROFILE = {
    "small": 1024,
    "medium": 1024 * 1024,
    "large": 10 * 1024 * 1024,
}
DEFAULT_SIZE_MIX = {
    "small": 0.7,
    "medium": 0.2,
    "large": 0.1,
}
DEFAULTS = {
    "name": "generated-workload",
    "total_requests": 100,
    "read_ratio": 0.9,
    "read_target_count": 10,
    "read_repeat_mode": "zipf",
    "zipf_alpha": 1.2,
    "size_profile": "small=1024,medium=1048576,large=10485760",
    "size_mix": "small:0.7,medium:0.2,large:0.1",
    "priority_policy": "uniform",
    "uniform_priority": 0,
    "priority_by_size": "small:2,medium:1,large:0",
    "domain_id": "default",
    "domain_mix": "",
}


def parse_size_spec(spec):
    values = {}
    for part in spec.split(","):
        if not part.strip():
            continue
        name, value = part.split("=", 1)
        values[name.strip()] = int(value.strip())
    return values


def parse_ratio_spec(spec):
    values = {}
    for part in spec.split(","):
        if not part.strip():
            continue
        name, value = part.split(":", 1)
        values[name.strip()] = float(value.strip())
    total = sum(values.values())
    if total <= 0:
        raise ValueError("ratios must sum to a positive value")
    return {name: ratio / total for name, ratio in values.items()}


def parse_priority_spec(spec):
    values = {}
    for part in spec.split(","):
        if not part.strip():
            continue
        name, value = part.split(":", 1)
        values[name.strip()] = int(value.strip())
    return values


def dict_to_size_spec(values):
    return ",".join(f"{key}={value}" for key, value in values.items())


def dict_to_ratio_spec(values):
    return ",".join(f"{key}:{value}" for key, value in values.items())


def load_workload_config(args):
    config = dict(DEFAULTS)
    if args.workload_json:
        with open(args.workload_json, "r", encoding="utf-8") as fh:
            workload = json.load(fh)
        config.update(workload)

    for key in DEFAULTS:
        value = getattr(args, key)
        if value is not None:
            config[key] = value

    if isinstance(config["size_profile"], dict):
        config["size_profile"] = dict_to_size_spec(config["size_profile"])
    if isinstance(config["size_mix"], dict):
        config["size_mix"] = dict_to_ratio_spec(config["size_mix"])
    if isinstance(config["priority_by_size"], dict):
        config["priority_by_size"] = dict_to_ratio_spec(config["priority_by_size"])

    args.name = config["name"]
    args.total_requests = int(config["total_requests"])
    args.read_ratio = float(config["read_ratio"])
    args.read_target_count = int(config["read_target_count"])
    args.read_repeat_mode = config["read_repeat_mode"]
    args.zipf_alpha = float(config["zipf_alpha"])
    args.size_profile = config["size_profile"]
    args.size_mix = config["size_mix"]
    args.priority_policy = config["priority_policy"]
    args.uniform_priority = int(config["uniform_priority"])
    args.priority_by_size = config["priority_by_size"]
    args.domain_id = config["domain_id"] or "default"
    args.domain_mix = config["domain_mix"]


def choose_weighted(items, weights, rng):
    point = rng.random()
    cumulative = 0
    for item, weight in zip(items, weights):
        cumulative += weight
        if point <= cumulative:
            return item
    return items[-1]


def choose_size(size_mix, rng):
    names = list(size_mix)
    weights = [size_mix[name] for name in names]
    return choose_weighted(names, weights, rng)


def choose_domain(args, domain_mix, rng):
    if not domain_mix:
        return args.domain_id
    names = list(domain_mix)
    weights = [domain_mix[name] for name in names]
    return choose_weighted(names, weights, rng)


def priority_for_size(size_label, args, priority_by_size):
    if args.priority_policy == "uniform":
        return args.uniform_priority
    if args.priority_policy == "small-first":
        return priority_by_size.get(size_label, args.uniform_priority)
    raise ValueError(f"unsupported priority policy: {args.priority_policy}")


def make_text_payload(size_bytes, label):
    prefix = f"curling-benchmark-{label}-{uuid.uuid4()}-".encode("utf-8")
    if len(prefix) >= size_bytes:
        return prefix[:size_bytes]
    return prefix + (b"x" * (size_bytes - len(prefix)))


def make_read_targets(args, size_profile, size_mix, rng):
    targets = []
    for index in range(args.read_target_count):
        size_label = choose_size(size_mix, rng)
        size_bytes = size_profile[size_label]
        target = f"read_{size_label}_{index:05d}"
        targets.append({
            "target_spec": target,
            "size_label": size_label,
            "size_bytes": size_bytes,
            "relative_path": f"{target}.txt",
        })
    return targets


def read_target_weights(targets, mode, alpha):
    if mode == "uniform":
        return [1 / len(targets)] * len(targets)
    if mode == "zipf":
        raw = [1 / ((rank + 1) ** alpha) for rank in range(len(targets))]
        total = sum(raw)
        return [value / total for value in raw]
    raise ValueError(f"unsupported read repeat mode: {mode}")


def choose_read_targets(targets, count, mode, alpha, rng):
    if mode == "unique":
        selected = []
        pool = list(targets)
        while len(selected) < count:
            rng.shuffle(pool)
            selected.extend(pool[:count - len(selected)])
        return selected

    weights = read_target_weights(targets, mode, alpha)
    return [choose_weighted(targets, weights, rng) for _ in range(count)]


def make_trace(args, read_targets, size_profile, size_mix, rng):
    read_count = round(args.total_requests * args.read_ratio)
    write_count = args.total_requests - read_count
    priority_by_size = parse_priority_spec(args.priority_by_size)
    domain_mix = parse_ratio_spec(args.domain_mix) if args.domain_mix else {}

    requests = []
    for target in choose_read_targets(read_targets, read_count, args.read_repeat_mode, args.zipf_alpha, rng):
        requests.append({
            "type": "read",
            "domain_id": choose_domain(args, domain_mix, rng),
            "target_spec": target["target_spec"],
            "size_label": target["size_label"],
            "size_bytes": target["size_bytes"],
            "priority": priority_for_size(target["size_label"], args, priority_by_size),
        })

    for index in range(write_count):
        size_label = choose_size(size_mix, rng)
        size_bytes = size_profile[size_label]
        input_path = f"writes/write_{size_label}_{index:05d}.bin"
        requests.append({
            "type": "write",
            "domain_id": choose_domain(args, domain_mix, rng),
            "target_spec": f"write_target_{index:05d}_{uuid.uuid4().hex[:8]}",
            "input_path": input_path,
            "size_label": size_label,
            "size_bytes": size_bytes,
            "priority": priority_for_size(size_label, args, priority_by_size),
        })

    rng.shuffle(requests)
    return {
        "name": args.name,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_requests": args.total_requests,
        "read_ratio": args.read_ratio,
        "read_target_count": args.read_target_count,
        "read_repeat_mode": args.read_repeat_mode,
        "zipf_alpha": args.zipf_alpha,
        "size_profile": size_profile,
        "size_mix": size_mix,
        "priority_policy": args.priority_policy,
        "priority_by_size": priority_by_size if args.priority_policy == "small-first" else {},
        "domain_id": args.domain_id,
        "domain_mix": domain_mix,
        "requests": requests,
    }

def load_trace(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def materialize_private_reads(trace, private_export_dir):
    export_dir = Path(private_export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)
    targets = {}
    for request in trace["requests"]:
        if request["type"] != "read":
            continue
        targets[request["target_spec"]] = request

    for target, request in targets.items():
        path = export_dir / f"{target}.txt"
        path.write_bytes(make_text_payload(int(request["size_bytes"]), target))
    return len(targets)


def materialize_client_writes(trace, write_input_dir):
    base_dir = Path(write_input_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for request in trace["requests"]:
        if request["type"] != "write":
            continue
        input_path = Path(request["input_path"])
        if input_path.parts and input_path.parts[0] == "writes":
            input_path = Path(*input_path.parts[1:])
        path = base_dir / input_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(make_text_payload(int(request["size_bytes"]), request["target_spec"]))
        count += 1
    return count


def generate_trace(args):
    load_workload_config(args)
    if args.read_target_count <= 0:
        raise ValueError("read-target-count must be positive")
    if not 0 <= args.read_ratio <= 1:
        raise ValueError("read-ratio must be between 0 and 1")

    rng = random.Random(args.seed)
    size_profile = parse_size_spec(args.size_profile)
    size_mix = parse_ratio_spec(args.size_mix)
    missing_sizes = set(size_mix) - set(size_profile)
    if missing_sizes:
        raise ValueError(f"size mix references undefined size labels: {sorted(missing_sizes)}")

    read_targets = make_read_targets(args, size_profile, size_mix, rng)
    trace = make_trace(args, read_targets, size_profile, size_mix, rng)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    trace_path = output_dir / f"{args.name}.trace.json"
    targets_path = output_dir / f"{args.name}.read_targets.json"
    trace_path.write_text(json.dumps(trace, indent=2, sort_keys=True), encoding="utf-8")
    targets_path.write_text(json.dumps(read_targets, indent=2, sort_keys=True), encoding="utf-8")

    print(f"wrote trace: {trace_path}")
    print(f"wrote read target metadata: {targets_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark traces and materialize role-local benchmark files.")
    parser.add_argument("--role", choices=["admin", "private", "client"], default="admin")
    parser.add_argument("--workload-json", help="Workload config JSON used by admin role.")
    parser.add_argument("--name")
    parser.add_argument("--trace-json", help="Trace generated by --role admin. Required for private/client roles.")
    parser.add_argument("--private-export-dir", help="Private node directory used by private storage for read exports.")
    parser.add_argument("--write-input-dir", help="Client node directory for generated write input files.")
    parser.add_argument("--output-dir", default="experiments/benchmark/generated")
    parser.add_argument("--total-requests", type=int)
    parser.add_argument("--read-ratio", type=float)
    parser.add_argument("--read-target-count", type=int)
    parser.add_argument("--read-repeat-mode", choices=["uniform", "zipf", "unique"])
    parser.add_argument("--zipf-alpha", type=float)
    parser.add_argument("--size-profile")
    parser.add_argument("--size-mix")
    parser.add_argument("--priority-policy", choices=["uniform", "small-first"])
    parser.add_argument("--uniform-priority", type=int)
    parser.add_argument("--priority-by-size")
    parser.add_argument("--domain-id")
    parser.add_argument("--domain-mix")
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()

    if args.role == "admin":
        generate_trace(args)
        return

    if not args.trace_json:
        raise ValueError("--trace-json is required for private/client roles")
    trace = load_trace(args.trace_json)

    if args.role == "private":
        if not args.private_export_dir:
            raise ValueError("--private-export-dir is required for private role")
        count = materialize_private_reads(trace, args.private_export_dir)
        print(f"wrote {count} read files to {Path(args.private_export_dir)}")
        return

    if args.role == "client":
        if not args.write_input_dir:
            raise ValueError("--write-input-dir is required for client role")
        count = materialize_client_writes(trace, args.write_input_dir)
        print(f"wrote {count} write input files to {Path(args.write_input_dir)}")
        return


if __name__ == "__main__":
    main()
