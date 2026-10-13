import * as cdk from "aws-cdk-lib";
import * as pipelines from "aws-cdk-lib/pipelines";
import * as secrets from "aws-cdk-lib/aws-secretsmanager";
import { Construct } from "constructs";
import { Stage } from "@gnome-trading-group/gnome-shared-cdk";
import { CONFIGS, GITHUB_BRANCH, GITHUB_REPO, PipelinesConfig } from "./config";
import { DataStack } from "./stacks/data-stack";

class AppStage extends cdk.Stage {
  constructor(scope: Construct, id: string, config: PipelinesConfig) {
    super(scope, id, { env: config.account.environment });

    new DataStack(this, "PipelinesDataStack", { stage: config.account.stage });
  }
}

export class PipelinesPipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const npmSecret = secrets.Secret.fromSecretNameV2(this, "NPMToken", "npm-token");

    const pipeline = new pipelines.CodePipeline(this, "ETLPipelinesPipeline", {
      crossAccountKeys: true,
      pipelineName: "ETLPipelinesPipeline",
      synth: new pipelines.ShellStep("Synth", {
        input: pipelines.CodePipelineSource.gitHub(GITHUB_REPO, GITHUB_BRANCH),
        commands: [
          'echo "//npm.pkg.github.com/:_authToken=${NPM_TOKEN}" > ~/.npmrc',
          "cd cdk/",
          "npm ci",
          "npx cdk synth",
        ],
        env: {
          NPM_TOKEN: npmSecret.secretValue.unsafeUnwrap(),
        },
        primaryOutputDirectory: "cdk/cdk.out",
      }),
    });

    pipeline.addStage(new AppStage(this, "Dev", CONFIGS[Stage.DEV]!));
    pipeline.addStage(new AppStage(this, "Prod", CONFIGS[Stage.PROD]!), {
      pre: [new pipelines.ManualApprovalStep("ApproveProd")],
    });

    pipeline.buildPipeline();
    npmSecret.grantRead(pipeline.synthProject.role!!);
    npmSecret.grantRead(pipeline.pipeline.role);
  }
}
