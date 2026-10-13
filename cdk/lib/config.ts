import { GnomeAccount, Stage } from "@gnome-trading-group/gnome-shared-cdk";

export interface PipelinesConfig {
  account: GnomeAccount;
}

export const CONFIGS: { [stage in Stage]?: PipelinesConfig } = {
  [Stage.DEV]: {
    account: GnomeAccount.InfraDev,
  },
  [Stage.PROD]: {
    account: GnomeAccount.InfraProd,
  },
};

export const GITHUB_REPO = "gnome-trading-group/gnome-pipelines";
export const GITHUB_BRANCH = "main";
