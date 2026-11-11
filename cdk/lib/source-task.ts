import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as ecr from "aws-cdk-lib/aws-ecr";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import { Construct } from "constructs";
import { grantResearchTables } from "./research-tables";

/** A polling source (gnome_pipelines/families/<family>.py SOURCES): which image runs it and which secrets it reads.
 * Its size, parameters and schedules come from the manifest; starting and stopping it is the Pipelines page's. */
export interface PollingSourceConfig {
  source: string;
  family: string;
  /** Names under gnome/pipelines/ its task may read: the source's `secrets`. */
  secrets: string[];
}

export interface SourceTaskProps {
  config: PollingSourceConfig;
  stage: string;
  repository: ecr.IRepository;
  executionRole: iam.IRole;
  logGroup: logs.ILogGroup;
  runsTable: dynamodb.ITable;
  tableBucketArn: string;
  registryApiKeyId: string;
  containerName: string;
}

/** A polling source's task definition: its family's image and runner, under a role that reads only its own
 * secrets. The trigger starts runs on it and the status Lambda restarts them. */
export class SourceTask extends Construct {
  public readonly taskDefinition: ecs.FargateTaskDefinition;
  public readonly taskRole: iam.Role;

  constructor(scope: Construct, id: string, props: SourceTaskProps) {
    super(scope, id);
    const { config } = props;
    const stack = cdk.Stack.of(this);

    this.taskRole = new iam.Role(this, "TaskRole", {
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
      description: `gnome-pipelines polling source ${config.source}`,
    });
    // runner.py moves the run to RUNNING.
    props.runsTable.grantReadWriteData(this.taskRole);
    s3.Bucket.fromBucketName(this, "ResearchBucket", `gnome-research-${props.stage}`).grantReadWrite(this.taskRole);
    grantResearchTables(stack, this.taskRole, props.tableBucketArn, { write: true });
    for (const name of config.secrets) {
      secretsmanager.Secret.fromSecretNameV2(this, `Secret-${name}`, `gnome/pipelines/${name}`).grantRead(this.taskRole);
    }
    this.taskRole.addToPolicy(new iam.PolicyStatement({
      actions: ["apigateway:GET"],
      resources: [`arn:aws:apigateway:${stack.region}::/apikeys/${props.registryApiKeyId}`],
    }));

    // The task size here is only a default: tasks.py sizes each run from the source's declared resources.
    this.taskDefinition = new ecs.FargateTaskDefinition(this, "TaskDefinition", {
      family: `gnome-pipelines-${config.source}`,
      cpu: 256,
      memoryLimitMiB: 512,
      taskRole: this.taskRole,
      executionRole: props.executionRole,
    });
    this.taskDefinition.addContainer(props.containerName, {
      image: ecs.ContainerImage.fromEcrRepository(props.repository, `${config.family}-latest`),
      // The runs' log group and prefix, so the Pipelines page shows a source's logs like any run's.
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: "pipeline", logGroup: props.logGroup }),
      environment: {
        PIPELINES_TABLE: props.runsTable.tableName,
        GNOME_STAGE: props.stage,
        REGISTRY_API_KEY_ID: props.registryApiKeyId,
      },
      // serve.py pushes the source's state after SIGTERM; give it time before the SIGKILL.
      stopTimeout: cdk.Duration.seconds(60),
    });
  }
}
