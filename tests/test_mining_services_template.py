from backend.template_loader import get_template_loader


def test_mining_services_template_loads():
    loader = get_template_loader()
    template = loader.get_template("mining_services")
    assert template["id"] == "mining_services"
    assert template["name"] == "Mining Services"
    assert "mining_services" in template.get("company_types", [])
    assert "Revenue Visibility" in template["template_behavior"]["stage3_scoring_factors"]["quality"]


def test_mining_services_company_type_routes_to_template():
    loader = get_template_loader()
    company_types = {row["id"]: row for row in loader.list_company_types()}
    assert company_types["mining_services"]["default_template_id"] == "mining_services"

    selection = loader.resolve_template_selection(
        "Analyse a listed contract mining services and mineral drilling company with equipment hire and blasting services.",
        ticker="ASX:MSV",
        company_type="mining_services",
    )
    assert selection["template_id"] == "mining_services"
    assert selection["company_type"] == "mining_services"


def test_mining_services_detection_prefers_services_over_miner():
    loader = get_template_loader()
    selected = loader.detect_company_type(
        "Analyse a listed contract mining services business with mineral drilling, blast-hole services, equipment hire, mine-site maintenance, and contract mining customers.",
        ticker="ASX:MSV",
    )
    assert selected == "mining_services"
