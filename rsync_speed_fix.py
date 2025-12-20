from typing import List, Optional
import os
import shlex
try:
    from ssh_manager import _get_ssh_command, SSH_CONTROL_PATH
except ImportError:
    # Fallback or standalone usage
    SSH_CONTROL_PATH = None
    pass

class RsyncSpeedOptimizer:
    """
    Optimizes rsync flags and SSH options for maximum throughput based on
    file size and transfer type (new vs repair).
    """

    def __init__(self, total_size_bytes: int, is_repair: bool = False):
        self.total_size_bytes = total_size_bytes
        self.is_repair = is_repair
        self.is_large_file = total_size_bytes > 500 * 1024 * 1024  # > 500MB

    def build_flags(self, user_base_flags: Optional[List[str]] = None) -> List[str]:
        """
        Builds a clean list of rsync flags.
        """
        if user_base_flags is None:
            # Default safe base if none provided
            # -v: verbose (needed for parsing)
            # --info=progress2: needed for total progress
            flags = ["-v", "--info=progress2"]
        else:
            flags = list(user_base_flags)
            # Ensure essential flags are present
            if "--info=progress2" not in flags:
                flags.append("--info=progress2")
            if "-v" not in flags and "--verbose" not in flags:
                flags.append("-v")

        # 1. Archive Mode & Permissions
        # Instead of 'partially' stripping -a, we let it be.
        # If the user passed -a, they get -rlptgoD.
        # If they didn't, we add nothing extra for perms/times unless they ask.
        
        # 2. Performance Optimizations
        
        # In-place is good for large files and resuming
        if "--inplace" not in flags:
            flags.append("--inplace")

        # Compression (-z) is CPU intensive and redundant for already compressed media
        # We aggressively strip it for media transfers.
        self._strip_compression(flags)

        # Block Size
        # 128KB is the safe max for older rsync/ssh variants. 
        # 4MB causes crashes on some systems.
        if "--block-size" not in str(flags):
            flags.append("--block-size=131072")

        # --whole-file vs Delta
        # If it's a NEW transfer (not a repair), skipping the delta check (hashing)
        # significantly speeds up the start and reduces CPU load.
        # If it's a REPAIR, we MUST use delta (default) to find the bad chunks.
        if not self.is_repair:
             if "--whole-file" not in flags and "-W" not in flags:
                 flags.append("--whole-file")
        else:
            # If repairing, ensure we are checksumming
            if "--checksum" not in flags and "-c" not in flags:
                flags.append("--checksum")
            # Ensure we are NOT forcing whole-file
            if "--whole-file" in flags: flags.remove("--whole-file")
            if "-W" in flags: flags.remove("-W")

        # Large directory optimization (doesn't hurt valid for single files too)
        # Allows starting transfer before scanning infinite file lists
        if "--no-inc-recursive" not in flags and self.is_large_file:
             # Actually, for single huge files, inc-recursive doesn't matter much.
             # But for many small files it helps. 
             # Let's keep it simple: strict flags only.
             pass

        return flags

    def _strip_compression(self, flags: List[str]):
        """Removes compression flags which hurt speed on high-bandwidth links for media."""
        if '-z' in flags: flags.remove('-z')
        if '--compress' in flags: flags.remove('--compress')
        # Remove 'z' from combined flags like '-vaz' -> '-va'
        for i, f in enumerate(flags):
            if f.startswith('-') and not f.startswith('--') and 'z' in f:
                flags[i] = f.replace('z', '')

    def build_ssh_options(self, port: int) -> str:
        """
        Builds high-performance SSH options.
        """
        # Cipher: CHACHA20 is generally faster in software than AES-GCM without hardware support.
        # AES-GCM is fastest with hardware support (AES-NI).
        # We enable both.
        # Compression=no is critical for speed on fast links.
        ciphers = "chacha20-poly1305@openssh.com,aes128-gcm@openssh.com,aes128-ctr"
        
        base_opts = [
            f"-p {port}",
            f"-c {ciphers}",
            "-o Compression=no", 
            "-o StrictHostKeyChecking=no",
            "-o UserKnownHostsFile=/dev/null",
            "-o ServerAliveInterval=60",
            "-o ServerAliveCountMax=30"
        ]

        # Add Multiplexing if available (ControlPath)
        # This speeds up connection establishment for multiple files
        if SSH_CONTROL_PATH:
             base_opts.append("-o ControlMaster=auto")
             base_opts.append(f"-o ControlPath={shlex.quote(SSH_CONTROL_PATH)}")
             base_opts.append("-o ControlPersist=60s")

        return "ssh " + " ".join(base_opts)
