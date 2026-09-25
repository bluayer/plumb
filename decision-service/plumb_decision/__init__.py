"""Decision model sidecar for the Plumb adaptive scheduler agent (model plugins: Laya, Jev, ...)."""

import os
import sys

# grpc_tools emits absolute imports ("from plumb.decision.v1 import decision_pb2"), so the
# generated tree must be importable as a top-level package.
_GEN = os.path.join(os.path.dirname(__file__), "gen")
if _GEN not in sys.path:
    sys.path.insert(0, _GEN)
