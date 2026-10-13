import * as cdk from "aws-cdk-lib";
import * as s3tables from "aws-cdk-lib/aws-s3tables";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";

// Must match gnomepy.data.CatalogConfig defaults.
const NAMESPACES = ["research", "artifacts"];

export class DataStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
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
  }
}
