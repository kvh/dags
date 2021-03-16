# noqa

from snapflow import Snap


@Snap
def customer_lifetime_sales(txs):
    txs_df = txs.as_dataframe()
    return txs_df.groupby("customer")["amount"].sum().reset_index()


from snapflow import SqlSnap


@SqlSnap
def customer_lifetime_sales_sql():
    return "select customer, sum(amount) as amount from txs group by customer"
    # Can use jinja templates too
    # return template("sql/customer_lifetime_sales.sql", ctx)


from snapflow import run, graph_from_yaml

g = graph_from_yaml(
    """
nodes:
  - key: stripe_charges
    snap: stripe.extract_charges
    params:
      api_key: sk_test_4eC39HqLyjWDarjtT1zdp7dc
  - key: accumulated_stripe_charges
    snap: core.dataframe_accumulator
    input: stripe_charges
  - key: stripe_customer_lifetime_sales
    snap: customer_lifetime_sales
    input: accumulated_stripe_charges
"""
)

# print(g)
assert len(g._nodes) == 3

from snapflow import Environment
import snapflow_stripe as stripe

env = Environment(modules=[stripe])
run(g, env=env, node_timeout_seconds=5)

# Get the final output block
datablock = env.get_latest_output("stripe_customer_lifetime_sales")
print(datablock.as_dataframe())
df = datablock.as_dataframe()
assert df.shape == (2, 100)
