import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";

const READ_ACTIONS = [
  "s3tables:GetTableBucket",
  "s3tables:GetNamespace",
  "s3tables:ListNamespaces",
  "s3tables:ListTables",
  "s3tables:GetTable",
  "s3tables:GetTableMetadataLocation",
  "s3tables:GetTableData",
];
const WRITE_ACTIONS = [
  "s3tables:CreateNamespace",
  "s3tables:CreateTable",
  "s3tables:UpdateTableMetadataLocation",
  "s3tables:PutTableData",
];

/** Lets a role read (or also write) the research datasets and artifact index through gnomepy.data. */
export function grantResearchTables(
  stack: cdk.Stack, grantee: iam.IGrantable, tableBucketArn: string, opts: { write: boolean },
): void {
  grantee.grantPrincipal.addToPrincipalPolicy(new iam.PolicyStatement({
    actions: opts.write ? [...READ_ACTIONS, ...WRITE_ACTIONS] : READ_ACTIONS,
    resources: [tableBucketArn, `${tableBucketArn}/table/*`],
  }));
  // gnomepy.data finds the bucket and the artifact root through these (DataStack writes them).
  grantee.grantPrincipal.addToPrincipalPolicy(new iam.PolicyStatement({
    actions: ["ssm:GetParameters"],
    resources: [`arn:aws:ssm:${stack.region}:${stack.account}:parameter/gnome/research/*`],
  }));
}
