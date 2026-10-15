from decimal import Decimal

import numpy as np

from gnome_pipelines.runner import _storable


def test_outputs_become_dynamodb_types():
    outputs = {"n_maps": np.int64(812), "auc": 0.71, "skipped": {"no_market": 3}, "version": "20261015T000000Z"}

    assert _storable(outputs) == {"n_maps": 812, "auc": Decimal("0.71"), "skipped": {"no_market": 3},
                                  "version": "20261015T000000Z"}
