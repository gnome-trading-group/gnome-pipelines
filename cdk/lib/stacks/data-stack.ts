import * as cdk from "aws-cdk-lib";
import * as s3tables from "aws-cdk-lib/aws-s3tables";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { Stage } from "@gnome-trading-group/gnome-shared-cdk";

// Namespaces and parameter names must match gnomepy.data.CatalogConfig.
const NAMESPACES = ["research", "artifacts"];

export interface DataStackProps extends cdk.StackProps {
  stage: Stage;
}

export class DataStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: DataStackProps) {
    super(scope, id, props);

    // Maintenance stays on the S3 Tables defaults. Never add Iceberg tags, branches or history.expire.*
    // table properties: any of them makes snapshot management fail for the whole table.
    const bucket = new s3tables.CfnTableBucket(this, "ResearchTableBucket", {
      tableBucketName: "gnome-research-tables",
    });
    bucket.applyRemovalPolicy(cdk.RemovalPolicy.RETAIN);

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
  }
}
