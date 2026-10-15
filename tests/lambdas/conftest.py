import os
import sys
from pathlib import Path

# The handlers read these at import, as they do in Lambda.
os.environ.update({
    "AWS_DEFAULT_REGION": "us-east-1",
    "PIPELINES_TABLE": "test-pipelines",
    "CLUSTER_ARN": "arn:cluster",
    "TASK_DEFINITIONS": '{"cs2": "arn:task-definition/gnome-pipelines-cs2"}',
    "SUBNET_IDS": "subnet-a,subnet-b",
    "SECURITY_GROUP_ID": "sg-1",
    "TRIGGER_FUNCTION": "gnome-pipelines-trigger",
    "TRIGGER_FUNCTION_ARN": "arn:trigger",
    "SCHEDULER_ROLE_ARN": "arn:scheduler-role",
    "LOG_GROUP": "/gnome-pipelines/runs",
})
# The Lambda folder is uploaded as it is, so tests mustn't leave compiled files in it.
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).parents[2] / "cdk" / "lambda"))
