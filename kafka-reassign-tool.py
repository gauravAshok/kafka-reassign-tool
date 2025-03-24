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
retry_after = None

def rotate(lst, n):
    """Rotate a list by n positions."""
    return lst[n:] + lst[:n]


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


def get_temp_file_name(id, suffix = ""):
    with tempfile.NamedTemporaryFile(prefix=input_file + "-" + str(id) + suffix + "-", delete=False) as tmp:
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


def create_preferred_leader_json(topic, partition, id):
    data = {
        "partitions": [
            {"topic": topic, "partition": partition}
        ]
    }
    file_path = get_temp_file_name(id, "-preferred-leader")
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
    (stdout, stderr, code) = run(
        get_kafka_root() + "/bin/kafka-reassign-partitions.sh",
        [
            "--zookeeper",
            get_zk_url(),
            "--reassignment-json-file",
            assignment_file,
            "--execute",
        ]
        + (["--throttle", str(new_throttle)] if new_throttle else []),
    )
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


def next_throttle(throttles, i):
    if throttles:
        return (throttles[i:-1] + throttles[-1:])[0]
    else:
        return None

# returns a tuple (current_leader, current_replicas, isr)
def describe_topic(topic, partition):
    (stdout, stderr, code) = run(get_kafka_root() + '/bin/kafka-topics.sh', ['--zookeeper', get_zk_url(), '--describe', '--topic', topic])
    if code != 0:
        raise Exception("error while describing topic\ncode: {}\nstderr: {}".format(code, stderr))
    # example describe output is like:
    # Topic:persephone_job_updates	PartitionCount:3	ReplicationFactor:3	Configs:
	#   Topic: persephone_job_updates	Partition: 0	Leader: 11	Replicas: 13,12,11	Isr: 13,12,11
    for line in stdout:
        m = re.search("Partition: {}.*Leader: (\\d+).*Replicas: ([0-9,]+).*Isr: ([0-9,]+)".format(partition), line)
        if m is not None:
            leader = int(m.groups()[0])
            replicas = [int(x) for x in m.groups()[1].split(",")]
            isr = [int(x) for x in m.groups()[2].split(",")]
            return (leader, replicas, isr)
    return None


def get_current_leader(topic, partition):
    description = describe_topic(topic, partition)
    if not description:
        raise Exception("topic {} partition {} not found".format(topic, partition))
    return description[0]


def ensure_preferred_leader(id, topic, partition, preferred_leader):
    current_leader = get_current_leader(topic, partition)
    if preferred_leader == current_leader:
        logging.info("preffered leader is already the leader")
        return
    file = create_preferred_leader_json(topic, partition, id)
    (stdout, stderr, code) = run(get_kafka_root() + '/bin/kafka-preferred-replica-election.sh',
                                ['--zookeeper', get_zk_url(), '--path-to-json-file', file])
    if code != 0:
        raise Exception("error while ensuring preferred leader\ncode: {}\nstderr: {}".format(code, stderr))
    msg = [x for x in stdout if "Successfully started preferred replica election" in x]
    started = True if len(msg) == 1 else False
    logging.debug("preferred leader output: \n%s\nstarted: %s", "\n".join(stdout), started)
    if not started:
        raise Exception("couldn't find the started message when starting the preferred leader election for topic {} partition {}".format(topic, partition))
    retry = 0
    while retry < 12:
        time.sleep(5)
        new_leader = get_current_leader(topic, partition)
        if new_leader == preferred_leader:
            logging.info("preferred leader election successful")
            return
        else:
            logging.info("preferred leader didnt change. It is still: {}. retrying after 5 sec".format(new_leader))
        retry = retry + 1
    raise Exception("preferred leader election failed. new leader is still: {}".format(new_leader))


def ensure_replicas_isr_preferred_leader(id, topic, partition, preferred_leader):
    description = describe_topic(topic, partition)
    if not description:
        raise Exception("topic {} partition {} not found".format(topic, partition))
    (current_leader, current_replicas, current_isr) = description
    if len(current_replicas) < 3:
        raise Exception("replication factor is less than 3. current replicas: {}".format(current_replicas))
    if len(current_isr) < 3:
        raise Exception("isr is less than 3. current isr: {}".format(current_isr))
    if not preferred_leader:
        return

    if preferred_leader not in current_replicas:
        raise Exception("preffered leader {} is not in current replicas {}".format(preferred_leader, current_replicas))
    if preferred_leader == current_leader:
        logging.info("preffered leader is already the leader")
        return
    logging.info("Preferred leader is not the current leader. Doing reassignment to make it leader")
    assignments = [
        {
            "topic": topic,
            "partition": partition,
            "to": [preferred_leader] + [x for x in current_replicas if x != preferred_leader]
        }
    ]
    reassign_partitions(assignments, id, None)
    logging.info("Reassignment done. Now ensuring preferred leader")
    ensure_preferred_leader(id, topic, partition, preferred_leader)


def reassign_partitions(assignments, id, throttles):
    # first verify that it has already been reassigned.
    file_path = create_assignment_json(assignments, id)
    (in_progress, completed, stdout, stderr) = verify_assignment(file_path)
    partitions = len(assignments)
    if completed == partitions:
        logging.info("reassignment completed successfully")
        return
    if in_progress == 0:
        logging.info("starting reassignment of partitions")
        begin_reassignment(file_path, next_throttle(throttles, 0))
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
        if throttles and retry_count % 2 == 0 and retry_count < (2 * len(throttles)):
            iteration = retry_count // 2
            new_throttle = next_throttle(throttles, iteration)
            logging.info("changing throttle to: %s", new_throttle)
            change_throttle(file_path, new_throttle)
            logging.info("changed throttle successfully")


# main method to do partition migration. It takes a batch.
def reassign(assignments, id, throttles):
    for assignment in assignments:
        topic = assignment["topic"]
        partition = assignment["partition"]
        preferred_leader = assignment.get("preferred_leader", None)
        if preferred_leader:
            logging.info("ensuring preferred leader for topic: %s partition: %s", topic, partition)
            ensure_replicas_isr_preferred_leader(id, topic, partition, preferred_leader)
        else:
            logging.info("reassigning topic: %s partition: %s", topic, partition)
            pass
    reassign_partitions(assignments, id, throttles)


def load_throttle_from_file():
    with open('throttle.json', 'r') as f:
        return json.load(f)


def get_script_progress(script_progress_file):
    if os.path.exists(script_progress_file):
        with open(script_progress_file, 'r') as f:
            return int(f.read())
    else:
        return 0


def save_script_progress(script_progress_file, index):
    with open(script_progress_file, 'w') as f:
        f.write(str(index))


def partition_reassignment(input_file, throttles):
    with open(input_file, 'r') as f:
        content = f.read()
        input_assignment = json.loads(content)
        script_progress_file = hashlib.md5(content.encode('utf-8')).hexdigest()

    start_index = get_script_progress(script_progress_file)
    for i in range(0, len(input_assignment)):
        if i < start_index:
            logging.info("skipping request %s as it was completed previously", i)
        else:
            # refetch throttle values
            save_script_progress(script_progress_file, i)
            reassign(input_assignment[i], i, throttles)


# file content is like:
# preferred_leader=<int>
# to=1,2,3
# from=4,5,6
# topic_1-0
# topic_1-1
# topic_2-1
def island_topics_reassignment(input_file, throttles):
    with open(input_file, 'r') as f:
        content = f.read()
    file_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
    lines = [l.strip() for l in content.splitlines()]
    preferred_leader = int(lines[0].split("=")[1])
    to = [int(x) for x in lines[1].split("=")[1].split(",")]
    from_replicas = [int(x) for x in lines[2].split("=")[1].split(",")]
    if len(to) != 3 or len(from_replicas) != 3:
        raise Exception("to and from replicas should be 3")
    if preferred_leader not in from_replicas:
        raise Exception("preferred leader should be in existing set of replicas")
    assignments = []
    for line in lines[3:]:
        # topic name can have hyphens. so split on last hyphen to get the partition number.
        (topic, partition) = line.rsplit("-", 1)
        partition = int(partition)
        (leader, replicas, isr) = describe_topic(topic, partition)
        logging.info("Fetched topic details: %s partition: %s leader: %s replicas: %s isr: %s", topic, partition, leader, replicas, isr)
        # replicas should match set-wise with the from_replicas
        if set(replicas) != set(from_replicas):
            raise Exception("replicas mismatch. expected: {} actual: {}".format(from_replicas, replicas))
        assignments.append([{
            "topic": topic,
            "partition": partition,
            "to": rotate(to, partition % 3),
            "preferred_leader": preferred_leader
        }])
    with open(file_hash, 'w') as f:
        f.write(json.dumps(assignments, indent=2))
    logging.info("Generated the partition assignment plan in file: %s", file_hash)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-home", help="Root directory of the Kafka installation. Default: {}".format(
        DEFAULT_KAFKA_ROOT), default=DEFAULT_KAFKA_ROOT)
    parser.add_argument(
        "--zookeeper", help="The connection string for the zookeeper connection. If not specified, an attempt to read it from Kafka config file is made")
    parser.add_argument("--input", help="File containing partition assignment", default=None)
    parser.add_argument("--island_topics_migration", help="File containing topics list from same island", default=None)
    parser.add_argument(
        "--throttle", help="Replication throttle in B/s. If not given, throttle.json file will be loaded", type=str, default=None)
    parser.add_argument("--retry-after", help="Retry duration in sec after which the tool should look for completion status again",
                        type=int, default=DEFAULT_RETRY_AFTER)
    parser.add_argument("--debug", help="For debug logs", action="store_true")

    args = parser.parse_args()

    set_logger(args.debug)

    kafka_root = args.kafka_home
    zookeeper_url = args.zookeeper
    retry_after = args.retry_after
    throttles = None
    if(args.throttle is None):
        throttles = load_throttle_from_file()
    else:
        throttles = [int(x) for x in json.loads(args.throttle)]
    
    logging.info("Using:")
    logging.info("kafka root: %s", kafka_root)
    logging.info("zk url: %s", get_zk_url())
    logging.info("throttle: %s", throttles)
    logging.info("first throttle: %s", next_throttle(throttles, 0))
    logging.info("retry after: %s", retry_after)

    input_file = args.input
    island_topics_migration = args.island_topics_migration
    if input_file:
        partition_reassignment(input_file, throttles)
    elif island_topics_migration:
        island_topics_reassignment(island_topics_migration, throttles)
    else:
        raise Exception("No input file provided")
