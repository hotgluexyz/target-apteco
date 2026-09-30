# target-apteco

Singer target that loads data into the [Apteco CDP](https://help.apteco.com/en/orbit/connect/) via Orbit Connect API.

Built for the Blackbaud Raiser's Edge → Apteco demo ([HGI-11341](https://linear.app/hotglue/issue/HGI-11341)):

| Source stream | Apteco CDP |
| --- | --- |
| `Contacts` / `constituents` | Individuals |
| `Transactions` / `gifts` | Individual Transactions (`Gift`) |

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Config

```json
{
  "username": "user@example.com",
  "password": "...",
  "base_url": "https://hotglue.ca-1.apteco.cloud",
  "data_view_name": "DB01",
  "cdp_source_name": "Blackbaud Raiser's Edge"
}
```

| Setting | Required | Default | Description |
| --- | --- | --- | --- |
| `username` | yes | | Orbit login (email) |
| `password` | yes | | Orbit password |
| `base_url` | no | `https://hotglue.ca-1.apteco.cloud` | Apteco cloud host |
| `data_view_name` | no | `DB01` | Orbit DataView |
| `cdp_source_name` | no | `Blackbaud Raiser's Edge` | CDP source label for identity resolution |

Auth uses OrbitAPI `SimpleLogin`; writes go through OrbitConnectAPI (temporary file → data source → data import → CDP table mapping → CDP import).

## Usage

```bash
target-apteco --about
tap-blackbaud --config tap.json | target-apteco --config .secrets/config.json
```

Accepted stream aliases: `Contacts`, `constituents`, `Transactions`, `gifts`, `donations`.

## Development

```bash
pytest tests/test_core.py -q
```
