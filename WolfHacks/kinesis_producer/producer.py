"""Run from repo root: python -m kinesis_producer.producer --sink stdout."""
import argparse
import os
import time
import urllib.request
from pathlib import Path
from kinesis_producer.mock import MockGenerator

def main():
    from dotenv import load_dotenv
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--sink", choices=["stdout", "file", "api", "kinesis"], default="stdout")
    parser.add_argument("--scenario", choices=["normal", "rise", "spike", "missing_rr"], default="normal")
    parser.add_argument("--count", type=int, default=120)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--session-id", default="demo-session")
    parser.add_argument("--url", default="http://127.0.0.1:8000/api/telemetry")
    parser.add_argument("--output", default="data/mock.jsonl")
    args = parser.parse_args()
    if args.count < 1 or args.interval < 0:
        parser.error("count must be positive and interval nonnegative")
    generator = MockGenerator(scenario=args.scenario, session_id=args.session_id)
    client = None
    if args.sink == "kinesis":
        import boto3
        client = boto3.client("kinesis", region_name=os.getenv("AWS_REGION", "us-east-1"))
    if args.sink == "file":
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    for _ in range(args.count):
        event = generator.next()
        payload = event.model_dump_json()
        if args.sink == "stdout":
            print(payload, flush=True)
        elif args.sink == "file":
            with open(args.output, "a", encoding="utf-8") as out:
                out.write(payload+"\n")
        elif args.sink == "api":
            request = urllib.request.Request(args.url, data=payload.encode(), headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=10) as response:
                response.read()
        else:
            # SDK standard retries; retrying may duplicate records. Silver deduplicates event identity.
            client.put_record(StreamName=os.environ["KINESIS_STREAM_NAME"],
                PartitionKey=f"{event.user_id}:{event.session_id}", Data=payload.encode())
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
