from pydantic import BaseModel
from typing import List


class CMDBItServiceScheme(BaseModel):
    id: int
    name: str
    code: str
    is_active: bool


class CMDBProductScheme(BaseModel):
    id: int
    name: str
    code: str
    is_active: bool
    owner_ad_uid: str
    owner_email: str
    it_services: List[CMDBItServiceScheme]
