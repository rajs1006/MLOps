import copy
from datetime import timedelta

import pandas as pd
from feaststore.RequestDataSource import CustomRequestDataSource
from feast import Entity, FeatureService, FeatureView, Field
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64


# Common parameters to be appended on every params
_commonParams = dict(
    start="2022-03-01T23:00:00",
    end="2023-03-15T22:59:59",
    requestTimeZone="Bst",
    responseTimeZone="Bst",
    resolution="Hour",
    function="Average",
    decimals=8,
)

# ---------------------------- BM ----------------------
paramsBM = copy.copy(_commonParams)
paramsBM["ids"] = 4423

bm = Entity(name="bm", join_keys=["epoch_time"])

# NOTE : TO use customRequest data source one need to add this store in FEAST-features store
# and use that as a module
bm_source = CustomRequestDataSource(
    name="bm",
    path="/Series",
    params=paramsBM,
    field_mapping={"ie.pricing.imbalanceprice(4423)": "IE_BM_PRICE"},
    timestamp_field="DeliveryBst",
)

bm_fv = FeatureView(
    name="feature_bm",
    entities=[bm],
    ttl=timedelta(days=1460),
    online=True,
    source=bm_source,
)


# ---------------------------- ID1 ----------------------
paramsID1 = copy.copy(_commonParams)
paramsID1["ids"] = 4425


id1 = Entity(name="id1", join_keys=["epoch_time"])

id1_source = CustomRequestDataSource(
    name="id1",
    path="/Series",
    params=paramsID1,
    field_mapping={"ie.pricing.intraday1price(4425)": "IE_ID1_PRICE"},
    timestamp_field="DeliveryBst",
)

id1_fv = FeatureView(
    name="feature_id1",
    entities=[id1],
    ttl=timedelta(days=1460),
    online=True,
    source=id1_source,
)


# ---------------------------- DA ----------------------
paramsDA = copy.copy(_commonParams)
paramsDA["ids"] = 4424

da = Entity(name="da", join_keys=["epoch_time"])

da_source = CustomRequestDataSource(
    name="da",
    path="/Series",
    params=paramsDA,
    field_mapping={"ie.pricing.dayaheadprice(4424)": "IE_DA_PRICE"},
    timestamp_field="DeliveryBst",
)

da_fv = FeatureView(
    name="feature_da",
    entities=[da],
    ttl=timedelta(days=1460),
    online=True,
    source=da_source,
)

# ---------------------------- SPREAD ----------------------
@on_demand_feature_view(
    sources=[bm_fv, id1_fv],
    schema=[
        Field(name="IE_SPREAD_ID1_VS_BM", dtype=Float64),
    ],
)
def spread_fv(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    print(inputs.columns)
    df["IE_SPREAD_ID1_VS_BM"] = inputs["IE_BM_PRICE"] - inputs["IE_ID1_PRICE"]
    return df


# -----------------------------------------------------------
featureService = FeatureService(
    name="ireland_dataleaks_service",
    features=[spread_fv, da_fv, bm_fv, id1_fv],
)
