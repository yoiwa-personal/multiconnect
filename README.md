# multiconnect: A TCP Proxy with Concurrent Connectivity Trials

`multiconnect` is a Python-based TCP proxy backend that concurrently attempts connections to multiple hosts or DNS names, automatically selecting the first available connection.

## Key Features

* **Concurrent Connections with Happy Eyeballs:** Initiates connection attempts to multiple address candidates concurrently or with a staggered delay, choosing the earliest available connection.
* **Dual-Stack IP Version Control:** Supports explicitly forcing or restricting connection attempts to IPv4 or IPv6 on a per-host or global basis.
* **Subnet-Aware Connection Filtering:** Evaluates the destination IP against a specified subnet mask before attempting a connection. If the resolved destination does not belong to the expected local network, the connection trial is automatically skipped.
* **Cross-Platform Compatibility:** Written in pure Python. It runs seamlessly across various operating systems, including Windows (Win32) without requiring a C compiler.
* **Socket Passing Mode:** It can either perform bidirectional proxying, or pass a connected TCP socket back to the caller process.

## Typical Use Cases

* **Optimizing Dual-Stack Connectivity:** Prioritizing IPv6 paths while maintaining a fast fallback to IPv4 for dual-stack hosts.
* **Handling Split-Horizon DNS and Hairpin NAT:** Useful when a target server sits behind a NAPT firewall or reverse proxy. By using the *subnet-aware filtering* feature, the client can list both private and public target addresses; it will automatically skip private address attempts when outside the private network, and still prioritize the direct connection inside the private network (avoiding hairpin NAT issues).
* **Supporting Legacy Clients:** Allowing older applications or clients that lack native IPv6 capabilities to connect to modern dual-stack or IPv6-only network environments via this proxy.
* **Preference-Based Multi-Routing:** Defining a strict priority order among multiple backup destinations by introducing custom connection delays.

## Usage

    usage: multiconnect.py [-h] [-4] [-6] [-v] [--delay DELAY] [-q] hostspec [hostspec ...]
    
    A TCP proxy that chooses the first available connection from multiple 
    destination candidates.
    
    positional arguments:
      hostspec              Connection destination candidates (syntax: '[delay:][protocol:]host[/mask_bits]:port').
    
    options:
      -h, --help            Show this help message and exit
      -4, --use-v4-only     Force the use of IPv4 addresses only.
      -6, --use-v6-only     Force the use of IPv6 addresses only.
      -v, --verbose         Increase verbosity level for diagnostics.
      --delay DELAY, --happy-eyeballs-delay DELAY
                            Staggered delay period (in seconds) between multiple IP address attempts (default: 0.25).
      -q, --quiet           Suppress progress/diagnostic messages
      --pass-fd             Enable socket passing mode.
      --pass-to-pid PID     Enable socket passing mode.


### Host Specifications

The syntax for each host specification is:

    [<delay>:][<protocol>:]<host>[/<mask_bits>]:<port>

In its simplest form, you can specify just the host and port (e.g., `example.com:22`). 
Literal IPv6 addresses containing colons must be enclosed in square brackets (e.g., `[::1]:22`).

You can customize the connection behavior using the following modifiers:

* **`<delay>:`** 
  Specifies a delay in seconds (e.g., `0.5`) before attempting to connect to this host. This allows preceding hosts in the list to be prioritized.
  If a connection attempt to a preceding host fails before the delay expires, the remaining delay is skipped, and the connection attempt to the next host starts immediately.
* **`<protocol>:`** 
  Prefix with `v4:` or `v6:` to restrict the connection to a specific IP version.
* **`/<mask_bits>:`** 
  Specifies the subnet mask bits (IPv4 or IPv6) for the expected local network. The script will check if the resolved destination IP address falls within the local network defined by this mask. If it does not match, the connection attempt for this spec is skipped.

### Global Options

* **`-4` / `-6`**: Forces the script to use only the corresponding IP version, overriding any DNS results or specific host definitions.
* **`--delay`**: Defines the staggered delay period (in seconds) between connection attempts for multiple IP addresses resolved from a single hostname.
* **`--pass-fd``, `--pass-to-pid`**: Enables socket passing mode.  See [socket-passing.md](socket-passing.md) for details.

## CLI Examples

### 1. Handling Hairpin NAT / Split-Horizon Environments
When your server is accessible via a public IP (`192.0.2.80`) from the outside, but via a private IP (`192.168.1.80`) when you are inside the office, you can pass both destinations. By adding the subnet mask (`/24`), `multiconnect` will smartly evaluate the network and skip inappropriate connection attempts:

    python3 multiconnect.py 192.168.1.80/24:22 1.0:192.0.2.80:22

### 2. Dual-Stack IPv4/IPv6 Connection (Fully Automated)
You don't need to specify IP versions manually. Just provide the hostname, and `multiconnect` will automatically resolve both IPv6 and IPv4 addresses, attempting connections concurrently to choose the earliest available one:

    python3 multiconnect.py example.com:80

### 3. Adding Connection Preferences with Protocol Restrictions
If you want to try a dual-stack primary server first, but want to restrict the backup server to IPv4 only and delay its attempt by 0.5 seconds:

    python3 multiconnect.py primary-server.example.com:8080 0.5:v4:backup-server.example.net:8080

### 4. Specifying a Subnet Mask with a Literal IPv6 Address
You can combine delays, subnet masks, and literal IPv6 addresses (enclosed in square brackets) for precise local network filtering:

    python3 multiconnect.py 0.5:[2001:db8::1]/64:80

## Integration Configurations

This script is highly effective when integrated as a proxy backend for SSH clients and other networking tools.

### 1. OpenSSH (`.ssh/config`)
To use `multiconnect` with OpenSSH, add a `ProxyCommand` directive to your target `Host` section in `.ssh/config`. 

For example, to configure a primary server with local subnet awareness, a staggered fallback, and a secondary backup server:

    Host primary-server.example.com
        ProxyCommand python3 /path/to/multiconnect.py 192.168.1.80/24:22 0.5:primary-server.example.com:22 1.0:v4:backup-server.example.net:22

### 2. PuTTY
You can configure PuTTY to use `multiconnect` as a local proxy. 

1. Navigate to **Connection** -> **Proxy** in the PuTTY configuration tree.
2. Select **Local** as the **Proxy type**.
3. In the **Proxy hostname** or **Telnet command, or local proxy command** field, enter the command line exactly as shown below.

Due to PuTTY's internal parsing rules, backslashes and percent signs must be doubled (`\\`) to correctly pass environment variables. The examples below are already formatted for PuTTY; **copy and paste them exactly as they are**.

* **Using an absolute path:**

    \\path\\to\\multiconnect.py 192.168.1.80/24:22 0.5:primary-server.example.com:22 1.0:v4:backup-server.example.net:22


* **Using a relative path with the Windows User Profile directory:**

    cmd /c %%USERPROFILE%%\\libexec\\multiconnect.py 192.168.1.80/24:22 0.5:primary-server.example.com:22 1.0:v4:backup-server.example.net:22

## Copyright and License

Copyright 2018-2026 Yutaka OIWA <yutaka@oiwa.jp>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
