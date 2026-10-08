# Third-Party Notices

BridgeOps is a source-available non-commercial project. This file records
project-level attribution and third-party notice requirements.

## Project references and acknowledgements

BridgeOps was developed with reference to ideas, operational workflows,
interface patterns, and API compatibility needs from the following projects:

- upstreamo / upstream-hub
  Reference: https://github.com/worryzyy/upstream-hub

- UpstreamOps
  Reference: upstream operations dashboard project lineage.

- Sub2API
  Reference: Sub2API-compatible upstream API behavior and UI patterns.

These acknowledgements do not imply endorsement by those projects.

## Upstream code and license compatibility

BridgeOps may include code, structures, or implementation ideas originating
from permissively licensed upstream work. Where code from an MIT-licensed
project is included or adapted, the original copyright and MIT license notice
must be preserved in source distributions and substantial binary
distributions. The BridgeOps non-commercial license applies only to
BridgeOps-owned additions and does not remove any rights granted by upstream
MIT-licensed code.

Sub2API is treated as an upstream compatibility target and UI/product
reference. Do not copy LGPL-licensed source code into BridgeOps unless the
copied files and distribution model are reviewed for LGPL compliance. If any
LGPL-covered code is ever included, it must remain under the LGPL-compatible
terms required by its original license.

## Runtime and build dependencies

Third-party Go and frontend dependencies are governed by their own licenses as
declared by their packages. The dependency manifests and lockfiles in this
repository identify the exact dependency graph used by the project:

- `go.mod`
- `go.sum`
- `frontend/package.json`
- `frontend/pnpm-lock.yaml`

Redistributors are responsible for complying with those dependency licenses.

### Lobe Icons

BridgeOps includes model-family and client-brand icon assets from Lobe Icons
(`@lobehub/icons-static-svg`):

MIT License

Copyright (c) 2023 LobeHub

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Required notice for modified distributions

If you distribute or publicly host a modified version of BridgeOps, include a
visible notice in the software and documentation similar to:

> This product is based on BridgeOps (https://github.com/Harvey-Mei/BridgesOps-pub)
> and includes modifications by the distributor.

Keep this file and `LICENSE` with source and binary distributions.

## Client brand assets

The unchanged CCSwitch, Chatbox and Continue images in `frontend/assets/client-icons/` retain their upstream MIT, GPL-3.0 and Apache-2.0 licenses respectively. Source links and the full license texts are included in that directory. These separately licensed assets and their trademarks are not relicensed under BridgeOps terms.
