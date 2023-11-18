import yaml
import subprocess as sp
import os


def read_yaml(path):
    """
    Reads YAML file.
    """
    with open(path, 'r', encoding='utf-8') as fr:
        data = fr.read()
    return yaml.safe_load(data)


def copy_to_s3(src_path, tgt_path):
    cmd_op = sp.run([f"aws s3 cp {src_path} {tgt_path}"], shell=True, capture_output=True)
    if cmd_op.returncode == 0:
        print(f"copy from {src_path} to {tgt_path} success.")
        print("copy output: \n", cmd_op.stdout.decode('utf-8'))
    else:
        raise Exception("Copy to S3 failed.")


def recursive_data_copy(src_loc, tgt_loc):
    cmd_result = sp.run([f"aws s3 cp {src_loc} {tgt_loc} --recursive"], shell=True, capture_output=True)
    if cmd_result.returncode == 0:
        print(f"copy from {src_loc} to {tgt_loc} success.")
        print("copy output: \n", cmd_result.stdout.decode('utf-8'))
    else:
        raise Exception("Copy to S3 failed.")

def get_account_id():
    return str(
        os.popen(
            "curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .accountId"
        )
        .read()
        .strip()
    )