import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecr from "aws-cdk-lib/aws-ecr";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as scheduler from "aws-cdk-lib/aws-scheduler";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import { Construct } from "constructs";
import { MonitoringFacade, noopAction } from "cdk-monitoring-constructs";
import { Stage } from "@gnome-trading-group/gnome-shared-cdk";

// gnome-controller's API Gateway routes /research/pipelines to these functions by name.
export const FUNCTION_NAMES = {
  list: "gnome-pipelines-list",
  create: "gnome-pipelines-create",
  get: "gnome-pipelines-get",
  update: "gnome-pipelines-update",
  trigger: "gnome-pipelines-trigger",
  status: "gnome-pipelines-status",
};
const NAME = "gnome-pipelines";
// cdk/lambda/store.py reads runs by task through this index.
const TASK_INDEX = "by-task";
// trigger_pipeline.py overrides the container by this name.
const CONTAINER_NAME = "pipeline";
// The registry's alerting stack names its bus, so any service can put alerts on it without depending on that stack.
const ALERT_BUS_NAME = "gnome-alerts";
const LAMBDA_RUNTIME = lambda.Runtime.PYTHON_3_13;
const GITHUB_SUBJECT = "repo:gnome-trading-group@160680294/gnome-pipelines@1410629960";

const TABLE_READ_ACTIONS = [
  "s3tables:GetTableBucket",
  "s3tables:GetNamespace",
  "s3tables:ListNamespaces",
  "s3tables:ListTables",
  "s3tables:GetTable",
  "s3tables:GetTableMetadataLocation",
  "s3tables:GetTableData",
];
const TABLE_WRITE_ACTIONS = [
  "s3tables:CreateNamespace",
  "s3tables:CreateTable",
  "s3tables:UpdateTableMetadataLocation",
  "s3tables:PutTableData",
];

export interface RunStackProps extends cdk.StackProps {
  stage: Stage;
  tableBucketArn: string;
  secrets: secretsmanager.ISecret[];
}

/** Everything that runs a pipeline: the image, the Fargate task, the run records, scheduling, and the API behind
 * the controller's Pipelines pages. */
export class RunStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: RunStackProps) {
    super(scope, id, props);

    const repository = new ecr.Repository(this, "ImageRepository", {
      repositoryName: NAME,
      lifecycleRules: [{ maxImageCount: 10 }],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // gnome-controller's BacktestStack creates the account's GitHub OIDC provider.
    const githubOidc = iam.OpenIdConnectProvider.fromOpenIdConnectProviderArn(
      this, "GithubOidcProvider", `arn:aws:iam::${this.account}:oidc-provider/token.actions.githubusercontent.com`,
    );
    const githubRole = new iam.Role(this, "GithubImageRole", {
      roleName: "gnome-pipelines-github-image",
      assumedBy: new iam.WebIdentityPrincipal(githubOidc.openIdConnectProviderArn, {
        StringEquals: {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
          // gnome-pipelines gets GitHub's ID-qualified subject (owner@id/repo@id), not the older owner/repo form.
          // A job tied to an environment gets the environment instead of the branch: the image workflow's
          // prod job (behind the prod environment's reviewers) pulls from dev ECR and pushes to prod.
          "token.actions.githubusercontent.com:sub": [
            `${GITHUB_SUBJECT}:ref:refs/heads/main`,
            `${GITHUB_SUBJECT}:environment:prod`,
          ],
        },
      }),
      description: "GitHub Actions in gnome-pipelines pushes the pipelines image",
    });
    repository.grantPush(githubRole);
    new cdk.CfnOutput(this, "GithubImageRoleArn", {
      value: githubRole.roleArn,
      description: "The AWS_ECR_ROLE_ARN_DEV / AWS_ECR_ROLE_ARN_PROD secret in the gnome-pipelines GitHub repo",
    });

    const table = new dynamodb.Table(this, "RunsTable", {
      tableName: NAME,
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    table.addGlobalSecondaryIndex({
      indexName: TASK_INDEX,
      partitionKey: { name: "ecs_task_arn", type: dynamodb.AttributeType.STRING },
    });

    const vpc = ec2.Vpc.fromLookup(this, "Vpc", { vpcName: "gnome-orchestrator-vpc" });
    const securityGroup = new ec2.SecurityGroup(this, "TaskSecurityGroup", {
      vpc,
      allowAllOutbound: true,
      description: "Pipeline Fargate tasks - outbound only",
    });
    const cluster = new ecs.Cluster(this, "Cluster", { vpc, clusterName: NAME });

    const taskRole = new iam.Role(this, "TaskRole", {
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
    });
    table.grantReadWriteData(taskRole);
    // Artifact files; the bucket belongs to gnome-controller's BacktestStack.
    s3.Bucket.fromBucketName(this, "ResearchBucket", `gnome-research-${props.stage}`).grantReadWrite(taskRole);
    taskRole.addToPolicy(new iam.PolicyStatement({
      actions: [...TABLE_READ_ACTIONS, ...TABLE_WRITE_ACTIONS],
      resources: [props.tableBucketArn, `${props.tableBucketArn}/table/*`],
    }));
    taskRole.addToPolicy(new iam.PolicyStatement({
      actions: ["ssm:GetParameters"],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter/gnome/research/*`],
    }));
    for (const secret of props.secrets) {
      secret.grantRead(taskRole);
    }

    const executionRole = new iam.Role(this, "TaskExecutionRole", {
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy")],
    });

    const taskDefinition = new ecs.FargateTaskDefinition(this, "TaskDefinition", {
      family: NAME,
      cpu: 1024,
      memoryLimitMiB: 4096,
      taskRole,
      executionRole,
    });
    taskDefinition.addContainer(CONTAINER_NAME, {
      image: ecs.ContainerImage.fromEcrRepository(repository, "latest"),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: "pipeline" }),
      environment: { PIPELINES_TABLE: table.tableName, GNOME_STAGE: props.stage },
    });

    const code = lambda.Code.fromAsset("lambda", { exclude: ["**/__pycache__"] });
    const fn = (id: string, functionName: string, module: string, description: string,
                environment: { [key: string]: string } = {}) => new lambda.Function(this, id, {
      functionName,
      runtime: LAMBDA_RUNTIME,
      code,
      handler: `${module}.handler`,
      memorySize: 256,
      timeout: cdk.Duration.seconds(30),
      description,
      environment: { PIPELINES_TABLE: table.tableName, ...environment },
    });

    const listFn = fn("ListFunction", FUNCTION_NAMES.list, "list_pipelines", "List pipeline definitions");
    const createFn = fn("CreateFunction", FUNCTION_NAMES.create, "create_pipeline", "Create a pipeline definition");
    const getFn = fn("GetFunction", FUNCTION_NAMES.get, "get_pipeline", "Get a pipeline definition and recent runs");
    table.grantReadData(listFn);
    table.grantReadWriteData(createFn);
    table.grantReadData(getFn);

    const triggerFn = fn("TriggerFunction", FUNCTION_NAMES.trigger, "trigger_pipeline",
      "Start a pipeline run (manual or scheduled)", {
        CLUSTER_ARN: cluster.clusterArn,
        TASK_DEFINITION_ARN: taskDefinition.taskDefinitionArn,
        SUBNET_IDS: vpc.publicSubnets.map((s) => s.subnetId).join(","),
        SECURITY_GROUP_ID: securityGroup.securityGroupId,
      });
    table.grantReadWriteData(triggerFn);
    triggerFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["ecs:RunTask"],
      resources: [taskDefinition.taskDefinitionArn],
    }));
    triggerFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["iam:PassRole"],
      resources: [taskRole.roleArn, executionRole.roleArn],
    }));

    const scheduleGroup = new scheduler.CfnScheduleGroup(this, "ScheduleGroup", { name: NAME });
    const schedulerRole = new iam.Role(this, "SchedulerRole", {
      assumedBy: new iam.ServicePrincipal("scheduler.amazonaws.com"),
    });
    triggerFn.grantInvoke(schedulerRole);

    const updateFn = fn("UpdateFunction", FUNCTION_NAMES.update, "update_pipeline",
      "Update a pipeline definition and its schedule", {
        TRIGGER_FUNCTION_ARN: triggerFn.functionArn,
        SCHEDULER_ROLE_ARN: schedulerRole.roleArn,
        SCHEDULE_GROUP: NAME,
      });
    table.grantReadWriteData(updateFn);
    updateFn.node.addDependency(scheduleGroup);
    updateFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["scheduler:CreateSchedule", "scheduler:UpdateSchedule", "scheduler:DeleteSchedule"],
      resources: [`arn:aws:scheduler:${this.region}:${this.account}:schedule/${NAME}/*`],
    }));
    updateFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["iam:PassRole"],
      resources: [schedulerRole.roleArn],
    }));

    const statusFn = fn("StatusFunction", FUNCTION_NAMES.status, "pipeline_status",
      "Fail runs whose task stopped abnormally and alert on failed runs");
    table.grantReadWriteData(statusFn);
    statusFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["events:PutEvents"],
      resources: [`arn:aws:events:${this.region}:${this.account}:event-bus/${ALERT_BUS_NAME}`],
    }));
    new events.Rule(this, "TaskStoppedRule", {
      eventPattern: {
        source: ["aws.ecs"],
        detailType: ["ECS Task State Change"],
        detail: { clusterArn: [cluster.clusterArn], lastStatus: ["STOPPED"] },
      },
      targets: [new targets.LambdaFunction(statusFn)],
    });

    // The alarms carry no action: the registry's alerting stack hears every alarm change through EventBridge and
    // reads the severity from the tier each is declared under.
    new MonitoringFacade(this, "PipelinesDashboard", {
      alarmFactoryDefaults: {
        actionsEnabled: true,
        alarmNamePrefix: "Pipelines-",
        action: noopAction(),
        datapointsToAlarm: 1,
      },
    })
      .addLargeHeader("Gnome Pipelines")
      // A scheduled run that fails to start never reaches the task-stopped rule that reports failed runs.
      .monitorLambdaFunction({
        lambdaFunction: triggerFn,
        humanReadableName: "Pipeline Trigger",
        alarmFriendlyName: "Trigger",
        addFaultCountAlarm: { Error: { maxErrorCount: 0 } },
      })
      .monitorLambdaFunction({
        lambdaFunction: statusFn,
        humanReadableName: "Pipeline Status",
        alarmFriendlyName: "Status",
        addFaultCountAlarm: { Error: { maxErrorCount: 0 } },
      });
  }
}
