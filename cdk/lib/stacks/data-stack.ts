import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3tables from "aws-cdk-lib/aws-s3tables";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { Stage } from "@gnome-trading-group/gnome-shared-cdk";
import { grantResearchTables } from "../research-tables";

// gnome-controller's API Gateway routes /research/datasets/* and /research/artifacts/* to this function by name.
export const DATA_API_FUNCTION = "gnome-pipelines-data-api";
// Namespaces and parameter names must match gnomepy.data.CatalogConfig.
const NAMESPACES = ["research", "artifacts"];
// Secrets for gnome_pipelines.credentials, as gnome/pipelines/<name>. Values are set by hand with
// put-secret-value; the placeholder must match credentials.PLACEHOLDER so an unset secret fails clearly.
const SECRETS = ["zenrows", "cfbd"];
const SECRET_PLACEHOLDER = "unset";

export interface DataStackProps extends cdk.StackProps {
  stage: Stage;
}

export class DataStack extends cdk.Stack {
  public readonly tableBucketArn: string;
  public readonly secrets: secretsmanager.ISecret[] = [];

  constructor(scope: Construct, id: string, props: DataStackProps) {
    super(scope, id, props);

    // Maintenance stays on the S3 Tables defaults. Never add Iceberg tags, branches or history.expire.*
    // table properties: any of them makes snapshot management fail for the whole table.
    const bucket = new s3tables.CfnTableBucket(this, "ResearchTableBucket", {
      tableBucketName: "gnome-research-tables",
    });
    bucket.applyRemovalPolicy(cdk.RemovalPolicy.RETAIN);
    this.tableBucketArn = bucket.attrTableBucketArn;

    for (const namespace of NAMESPACES) {
      const ns = new s3tables.CfnNamespace(this, `Namespace-${namespace}`, {
        tableBucketArn: bucket.attrTableBucketArn,
        namespace,
      });
      ns.applyRemovalPolicy(cdk.RemovalPolicy.RETAIN);
    }

    new ssm.StringParameter(this, "TableBucketArnParameter", {
      parameterName: "/gnome/research/table-bucket-arn",
      stringValue: bucket.attrTableBucketArn,
      description: "S3 Tables bucket holding the research datasets and artifact index (GNOME_TABLE_BUCKET_ARN)",
    });

    // The research bucket itself belongs to gnome-controller's BacktestStack.
    new ssm.StringParameter(this, "ArtifactRootParameter", {
      parameterName: "/gnome/research/artifact-root",
      stringValue: `s3://gnome-research-${props.stage}/artifacts`,
      description: "S3 prefix that artifact files are written under (GNOME_ARTIFACT_ROOT)",
    });

    for (const name of SECRETS) {
      const secret = new secretsmanager.Secret(this, `Secret-${name}`, {
        secretName: `gnome/pipelines/${name}`,
        secretStringValue: cdk.SecretValue.unsafePlainText(SECRET_PLACEHOLDER),
        description: "gnome-pipelines secret; set its value with put-secret-value",
      });
      secret.applyRemovalPolicy(cdk.RemovalPolicy.RETAIN);
      this.secrets.push(secret);
    }

    // A container image: gnomepy.data's pyarrow and pandas don't fit a zip Lambda.
    const dataApi = new lambda.DockerImageFunction(this, "DataApiFunction", {
      functionName: DATA_API_FUNCTION,
      code: lambda.DockerImageCode.fromImageAsset("data-api"),
      memorySize: 2048,
      timeout: cdk.Duration.seconds(60),
      description: "The datasets and artifacts API behind the controller's /research/datasets and /research/artifacts",
    });
    grantResearchTables(this, dataApi, bucket.attrTableBucketArn, { write: false });
    // Signs artifact download links; the bucket belongs to gnome-controller's BacktestStack.
    s3.Bucket.fromBucketName(this, "ResearchBucket", `gnome-research-${props.stage}`).grantRead(dataApi, "artifacts/*");
  }
}
