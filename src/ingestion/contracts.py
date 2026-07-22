"""
Stage 1a — the data contract.

Same Pydantic v2 pattern as Day 4's `RetailTransactionContract`, applied at the
Kafka ingestion boundary the way Day 2's `SensorReading` gate was applied to
consumed messages. Anything that fails this contract never reaches Bronze.
"""

import re

from pydantic import BaseModel, ConfigDict, field_validator


class RetailTransactionContract(BaseModel):
    """
    Machine-enforceable schema for one Online Retail invoice line.

    strict=False keeps the CSV/JSON string -> float coercion that the raw feed
    needs; every real business rule below is enforced explicitly instead.
    """

    model_config = ConfigDict(strict=False)

    InvoiceNo:   str
    StockCode:   str
    Description: str
    Quantity:    float
    UnitPrice:   float
    CustomerID:  str
    Country:     str
    InvoiceDate: str

    @field_validator("CustomerID")
    @classmethod
    def customer_id_required(cls, v: str) -> str:
        if not v or v.strip() in ("", "nan", "None"):
            raise ValueError("CustomerID is required — cannot be null")
        return v.strip()

    @field_validator("Quantity")
    @classmethod
    def positive_quantity(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"Quantity must be > 0 (got {v} — likely a cancellation)")
        return v

    @field_validator("UnitPrice")
    @classmethod
    def positive_price(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"UnitPrice must be > 0 (got {v})")
        return v

    @field_validator("InvoiceNo")
    @classmethod
    def valid_invoice(cls, v: str) -> str:
        if not re.match(r"^[A-Z]?\d{5,6}$", v.strip()):
            raise ValueError(f"InvoiceNo format invalid: '{v}'")
        return v.strip()

    @field_validator("InvoiceDate")
    @classmethod
    def invoice_date_present(cls, v: str) -> str:
        if not v or v.strip() in ("", "nan", "None"):
            raise ValueError("InvoiceDate is required — cannot be null")
        return v.strip()

    @field_validator("Description")
    @classmethod
    def description_not_null(cls, v: str) -> str:
        if not v or v.strip() in ("", "nan", "None"):
            raise ValueError("Description is required — cannot be null")
        return v.strip()


def business_key(record: dict) -> str:
    """
    The business key every downstream layer merges on.

    One invoice line is uniquely identified by (InvoiceNo, StockCode); the two
    are concatenated so the Delta MERGE condition stays a single-column match.
    """
    return f"{record['InvoiceNo']}_{record['StockCode']}"
