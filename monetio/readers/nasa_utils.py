import os
import stat
from netrc import netrc


def setup_netrc(username, password, machine="urs.earthdata.nasa.gov"):
    """
    Setup .netrc file with given credentials for a machine.

    Parameters
    ----------
    username : str
        NASA Earthdata username.
    password : str
        NASA Earthdata password.
    machine : str, optional
        Machine name, by default "urs.earthdata.nasa.gov".
    """
    netrc_path = os.path.expanduser("~/.netrc")

    # Check if file exists and has credentials for this machine
    if os.path.exists(netrc_path):
        try:
            n = netrc(netrc_path)
            if n.authenticators(machine):
                # Machine already exists in .netrc
                return
        except Exception:  # noqa: BLE001 - probing an existing credentials file; an unparsable one falls through to appending/creating, so the requested credentials are still written (categories: malformed, permission_denied)
            # Failed to parse, maybe it's empty or corrupt.
            # We'll try to append anyway or create a new one.
            pass

    # Ensure the directory exists
    os.makedirs(os.path.dirname(netrc_path), exist_ok=True)

    # Append to or create .netrc
    # We use a+ to read and append
    mode = "a" if os.path.exists(netrc_path) else "w"
    with open(netrc_path, mode) as f:
        # If appending, ensure we start on a new line
        if mode == "a":
            # Check if last line ends with newline
            try:
                with open(netrc_path, "rb") as rb:
                    rb.seek(-1, os.SEEK_END)
                    if rb.read(1) != b"\n":
                        f.write("\n")
            except OSError:
                pass
        f.write(f"machine {machine} login {username} password {password}\n")

    # Ensure proper permissions (600)
    os.chmod(netrc_path, stat.S_IRUSR | stat.S_IWUSR)
