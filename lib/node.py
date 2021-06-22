from typing import Protocol
from abc import abstractmethod
from pathlib import Path

from lib.node_config import NodeConfig

class Node(Protocol):
    # TODO: state which methods cannot be run in parallel
    # require the implementations to detect such parallel runs and throw errors
    @abstractmethod
    def start(self) -> None:
        """
        Start the node and wait for initialization.
        """
        # TODO: does nothing if the node is already running
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the node gracefully.
        Does nothing if the node is already not running.
        """
        raise NotImplementedError

    def restart(self) -> None:
        """
        Restart the node gracefully.
        If the node is already not running, simply start it.
        """
        self.stop()
        self.start()

    def pause(self) -> None:
        """
        Pause the server process.
        Does nothing if already paused or not running.
        """
        raise NotImplementedError

    def unpause(self) -> None:
        """
        Unpause the server process.
        Does nothing if already unpaused or not running.
        """
        raise NotImplementedError

    def hard_stop(self) -> None:
        """
        Stop the node non-gracefully (such as kill -9).
        Does nothing if already not running.
        """
        raise NotImplementedError

    def hard_restart(self) -> None:
        """
        Restart the node non-gracefully.
        If the node is already not running, simply start it.
        """
        self.hard_stop()
        self.start()

    # TODO: which IP (there may be multiple public IPs)?
    # also, dedicated IP type?
    def ip(self) -> str:
        """
        Return the node's public IP.
        """
        raise NotImplementedError

    def get_node_config(self) -> NodeConfig:
        """
        Get the node's current configuration (conf/scylla.yaml).
        """
        raise NotImplementedError

    def reset_node_config(self, cfg: NodeConfig) -> None:
        """
        Modify the node's configuration (conf/scylla.yaml).
        Warning: modifying some parameters is dangerous or unsupported (e.g. the node's IP).
        """
        raise NotImplementedError

    def reset_scylla_binary(self, binary_path: Path) -> None:
        """
        Replace the Scylla binary with the one provided under `binary_path`.
        Warning: do this only if you really know what you're doing.
        Ensure that the binary is compatible with the OS on which the node is running.
        """
        raise NotImplementedError
