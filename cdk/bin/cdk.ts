#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { GnomeAccount } from "@gnome-trading-group/gnome-shared-cdk";
import { PipelinesPipelineStack } from "../lib/pipelines-pipeline-stack";

const app = new cdk.App();
new PipelinesPipelineStack(app, "ETLPipelinesPipelineStack", {
  env: GnomeAccount.InfraPipelines.environment,
});
app.synth();
