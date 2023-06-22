# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

try:
    import mpi4py
except ImportError:
    raise ImportError(
        "Missing dependency 'mpi4py'. Use pip or conda to install it."
    ) from None

import unidist.core.backends.mpi.core.common as common

# TODO: Find a way to move this after all imports
mpi4py.rc(recv_mprobe=False, initialize=False)
from mpi4py import MPI  # noqa: E402

logger = common.get_logger("async_operations", "async_operations.log")


class AsyncOperations:
    """
    Class that stores MPI async communication handlers.

    Class holds a reference to sending data to prolong data lifetime during send operation.
    """

    __instance = None

    def __init__(self):
        # I-prefixed mpi call handlers
        self._send_async_handlers = []

    @classmethod
    def get_instance(cls):
        """
        Get instance of ``AsyncOperations``.

        Returns
        -------
        AsyncOperations
        """
        if cls.__instance is None:
            cls.__instance = AsyncOperations()
        return cls.__instance

    def append(self, handlers_list):
        """
        Append a new list of handlers to the current list.

        Parameters
        ----------
        handler_list : list
            A list of pairs with handler and data reference.
        """
        self._send_async_handlers.append(handlers_list)

    def check(self):
        """
        Check all MPI async send requests readiness and remove a reference to sending data.

        Notes
        -----
        The check on readiness is performed at once for a bunch of requests,
        which corresponds to a complete operation, e.g., ``isend_simple_operation``.
        """

        def is_ready(handler_list):
            return MPI.Request.Testall([h for h, _ in handler_list])

        self._send_async_handlers[:] = [
            hl for hl in self._send_async_handlers if not is_ready(hl)
        ]

    def finish(self):
        """Cancel all MPI async send requests, which have not been initiated yet."""
        # We iterate inversely to allow initial sends to complete.
        for handler_list in self._send_async_handlers[::-1]:
            tests = [h.Test() for h, _ in handler_list]
            cancel = all(not test for test in tests)
            if cancel:
                _ = [h.Cancel() for h, _ in handler_list]
                MPI.Request.Waitall([h for h, _ in handler_list])
        self._send_async_handlers.clear()
