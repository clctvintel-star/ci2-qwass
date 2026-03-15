import yaml


def load_firms_config(config_path="config/firms.yaml"):
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_discovery_queries(config):
    core_firms = config["firms"]["core"]

    discovery_queries = {
        "Citadel": [
            '"Citadel" hedge fund',
            '"Citadel Securities"',
        ],
        "Millennium": [
            '"Millennium" hedge fund',
            '"Millennium Management"',
        ],
        "Point72": [
            '"Point72"',
        ],
        "D. E. Shaw": [
            '"D. E. Shaw" hedge fund',
            '"The D. E. Shaw Group"',
        ],
        "Two Sigma": [
            '"Two Sigma"',
            '"Two Sigma Investments"',
        ],
        "Balyasny": [
            '"Balyasny"',
            '"Balyasny Asset Management"',
        ],
        "Schonfeld": [
            '"Schonfeld"',
            '"Schonfeld Strategic Advisors"',
        ],
        "ExodusPoint": [
            '"ExodusPoint"',
        ],
        "Jane Street": [
            '"Jane Street" trading firm',
            '"Jane Street Capital"',
        ],
        "Hudson River Trading": [
            '"Hudson River Trading"',
        ],
        "Jump Trading": [
            '"Jump Trading"',
            '"Jump Trading LLC"',
            '"Jump Crypto"',
        ],
    }

    query_map = {}
    for firm in core_firms:
        query_map[firm] = discovery_queries.get(firm, [f'"{firm}"'])

    return query_map


if __name__ == "__main__":
    config = load_firms_config()
    query_map = build_discovery_queries(config)

    for firm, queries in query_map.items():
        print(f"\n{firm}")
        for q in queries:
            print(f"  - {q}")
