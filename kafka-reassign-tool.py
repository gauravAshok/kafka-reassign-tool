import os
import os.path
import json
import subprocess
import tempfile
import argparse
import logging
import re
import time
import hashlib

DEFAULT_KAFKA_ROOT = '/usr/share/varadhi-kafka'
DEFAULT_KAFKA_CONFIG = '/config/server.properties'
DEFAULT_RETRY_AFTER = 60

input_file = None
kafka_root = None
zookeeper_url = None
input_assignment = None
throttle = None
throttle_getter = None
retry_after = None
script_progress_file = None

def set_logger(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(format='%(asctime)s %(message)s', level=level)


def output_to_lines(output):
    if output == None or output.strip() == "":
        return None
    return [x.strip() for x in output.splitlines()]


def run(command, args=[], check=False):
    cp = subprocess.run([command] + args, universal_newlines=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=check)
    return (output_to_lines(cp.stdout), output_to_lines(cp.stderr), cp.returncode)


def get_kafka_root():
    return kafka_root


def get_zk_url():
    global zookeeper_url

    if zookeeper_url is not None:
        return zookeeper_url

    config_file = get_kafka_root() + DEFAULT_KAFKA_CONFIG

    if os.path.isfile(config_file):
        logging.info("Reading %s", config_file)
        with open(config_file, 'r') as f:
            for l in f.read().splitlines():
                m = re.search("^zookeeper.connect=(.*)", l)
                if m is not None:
                    zookeeper_url = m.groups()[0]
                    break
    else:
        logging.info("Config file %s does not exist", config_file)

    if zookeeper_url is None:
        raise Exception("No zookeeper URL given")

    return zookeeper_url


def get_temp_file_name(id):
    with tempfile.NamedTemporaryFile(prefix=input_file + "-" + str(id) + "-", delete=False) as tmp:
        return tmp.name


def create_assignment_json(assignments, id):
    data = {
        "partitions": [
            {"topic": x["topic"], "partition": x["partition"], "replicas": x["to"]} for x in assignments
        ],
        "version": 1
    }
    file_path = get_temp_file_name(id)
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)
    logging.info("temp file: %s", file_path)
    logging.info("content: %s", json.dumps(data))
    return file_path


def verify_assignment(assignment_file):
    (stdout, stderr, code) = run(get_kafka_root() + '/bin/kafka-reassign-partitions.sh',
                                 ['--zookeeper', get_zk_url(), '--reassignment-json-file', assignment_file, '--verify'])
    if code != 0:
        raise Exception("error while verifying\ncode: {}\nstderr: {}".format(code, stderr))
    reassignment_lines = [x for x in stdout if x.startswith("Reassignment")]
    in_progress = len([x for x in reassignment_lines if x.endswith("is still in progress")])
    completed = len([x for x in reassignment_lines if x.endswith("completed successfully")])
    logging.debug("verify output: %s", "\n".join(stdout))
    logging.debug("in_progress: %s", in_progress)
    logging.debug("completed: %s", completed)
    return (in_progress, completed, stdout, stderr)


def begin_reassignment(assignment_file, new_throttle):
    (stdout, stderr, code) = run(get_kafka_root() + '/bin/kafka-reassign-partitions.sh',
                                ['--zookeeper', get_zk_url(), '--reassignment-json-file', assignment_file, '--execute', '--throttle', str(new_throttle)])
    if code != 0:
        raise Exception("error while begin assign\ncode: {}\nstderr: {}".format(code, stderr))
    msg = [x for x in stdout if "Successfully started reassignment of partitions" in x]
    started = True if len(msg) == 1 else False
    logging.debug("begin output: \n%s\nstarted: %s", "\n".join(stdout), started)
    if not started:
        logging.info("couldn't find the started message when starting the reassignment.")
        logging.info("tool output:\n%s", stdout)
        raise Exception("couldnt start the reassignment")


def change_throttle(assignment_file, new_throttle):
    (stdout, stderr, code) = run(get_kafka_root() + '/bin/kafka-reassign-partitions.sh',
                                ['--zookeeper', get_zk_url(), '--reassignment-json-file', assignment_file, '--execute', '--throttle', str(new_throttle)])
    if code != 0:
        raise Exception("error while begin assign\ncode: {}\nstderr: {}".format(code, stderr))
    msg = [x for x in stdout if "There is an existing assignment running" in x]
    changed = True if len(msg) == 1 else False
    logging.debug("change throttle output: \n%s\changed: %s", "\n".join(stdout), changed)
    if not changed:
        logging.info("couldn't find the existing assignment running msg while changing throttle.")
        logging.info("tool output:\n%s", stdout)
        raise Exception("couldnt change the throttle")


def next_throttle(i):
    return (throttle[i:-1] + throttle[-1:])[0]

def reassign(assignments, id):
    # first verify that it has already been reassigned.
    file_path = create_assignment_json(assignments, id)
    (in_progress, completed, stdout, stderr) = verify_assignment(file_path)
    partitions = len(assignments)
    if completed == partitions:
        logging.info("reassignment completed successfully")
        return
    if in_progress == 0:
        logging.info("starting reassignment of partitions")
        begin_reassignment(file_path, next_throttle(0))
        logging.info("started reassignment of partitions")

    retry_count = 0
    while True:
        time.sleep(retry_after)
        (in_progress, completed, stdout, stderr) = verify_assignment(file_path)
        if completed == partitions:
            logging.info("reassignment completed successfully")
            return
        elif in_progress > 0:
            logging.info("reassignemnt is still in progress. Retrying after %s seconds", retry_after)
        else:
            raise Exception("reassignment failed.\nstdout:\n%s\nstderr:\n%s", "\n".join(stdout), "\n".join("stderr"))
        retry_count = retry_count + 1
        if retry_count % 2 == 0 and retry_count < (2 * len(throttle)):
            iteration = retry_count // 2
            new_throttle = next_throttle(iteration)
            logging.info("changing throttle to: %s", new_throttle)
            change_throttle(file_path, new_throttle)
            logging.info("changed throttle successfully")


def load_throttle_from_file():
    with open('throttle.json', 'r') as f:
        return json.load(f)

def get_script_progress():
    if os.path.exists(script_progress_file):
        with open(script_progress_file, 'r') as f:
            return int(f.read())
    else:
        return 0

def save_script_progress(index):
    with open(script_progress_file, 'w') as f:
        f.write(str(index))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-home", help="Root directory of the Kafka installation. Default: {}".format(
        DEFAULT_KAFKA_ROOT), default=DEFAULT_KAFKA_ROOT)
    parser.add_argument(
        "--zookeeper", help="The connection string for the zookeeper connection. If not specified, an attempt to read it from Kafka config file is made")
    parser.add_argument("input", help="File containing partition assignment")
    parser.add_argument(
        "--throttle", help="Replication throttle in B/s. If not given, throttle.json file will be loaded", type=str, default=None)
    parser.add_argument("--retry-after", help="Retry duration in sec after which the tool should look for completion status again",
                        type=int, default=DEFAULT_RETRY_AFTER)
    parser.add_argument("--debug", help="For debug logs", action="store_true")

    args = parser.parse_args()

    kafka_root = args.kafka_home
    zookeeper_url = args.zookeeper
    input_file = args.input
    with open(input_file, 'r') as f:
        content = f.read()
        input_assignment = json.loads(content)
        script_progress_file = hashlib.md5(content.encode('utf-8')).hexdigest()
    retry_after = args.retry_after
    set_logger(args.debug)

    if(args.throttle is None):
        throttle_getter = load_throttle_from_file
    else:
        t = [int(x) for x in json.loads(args.throttle)]
        throttle_getter = lambda : t

    throttle = throttle_getter()

    logging.info("Using:")
    logging.info("kafka root: %s", kafka_root)
    logging.info("zk url: %s", get_zk_url())
    logging.info("throttle: %s", throttle)
    logging.info("first throttle: %s", next_throttle(0))
    logging.info("retry after: %s", retry_after)

    start_index = get_script_progress()
    for i in range(0, len(input_assignment)):
        if i < start_index:
            logging.info("skipping request %s as it was completed previously", i)
        else:
            # refetch throttle values
            save_script_progress(i)
            throttle = throttle_getter()
            reassign(input_assignment[i], i)
