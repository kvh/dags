from pprint import pprint

import dags_bi as bi
import dags_stripe as stripe
from dags import Environment, Graph
from dags.core.graph import Graph


def test_stripe(api_key):
    env = Environment()
    graph = Graph(env)
    graph.add_node(
        key="stripe_charges",
        pipe=stripe.pipes.extract_charges,
        config={"api_key": api_key},
    )
    graph.add_node(
        key="ltv_model",
        pipe=bi.pipes.transaction_ltv_model,
        upstream="stripe_charges",
        schema_mapping={"customer": "customer_id", "created": "transacted_at"},
    )
    ltv_output = env.produce(graph, "ltv_model", node_timelimit_seconds=10)
    print(ltv_output.as_dataframe())


if __name__ == "__main__":
    test_key = "sk_test_4eC39HqLyjWDarjtT1zdp7dc"
    api_key = input("Enter Stripe API key (default test key): ")
    if not api_key:
        api_key = test_key
    test_stripe(api_key)
