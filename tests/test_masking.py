"""Tests for sensitive field masking."""

import pytest

from maxc_cli.masking import mask_rows, _classify_column

pytestmark = pytest.mark.unit


class TestClassifyColumn:
    def test_phone(self):
        assert _classify_column("phone") == "phone"
        assert _classify_column("user_phone") == "phone"
        assert _classify_column("mobile_number") == "phone"
        assert _classify_column("tel") == "phone"
        assert _classify_column("cellphone") == "phone"

    def test_email(self):
        assert _classify_column("email") == "email"
        assert _classify_column("user_email") == "email"
        assert _classify_column("e_mail") == "email"
        assert _classify_column("mail_addr") == "email"

    def test_password(self):
        assert _classify_column("password") == "password"
        assert _classify_column("passwd") == "password"
        assert _classify_column("user_secret") == "password"
        assert _classify_column("pwd") == "password"
        assert _classify_column("api_key") == "password"

    def test_id_card(self):
        assert _classify_column("id_card") == "id_card"
        assert _classify_column("idcard") == "id_card"
        assert _classify_column("ssn") == "id_card"
        assert _classify_column("identity_no") == "id_card"
        assert _classify_column("cert_no") == "id_card"

    def test_non_sensitive(self):
        assert _classify_column("user_name") is None
        assert _classify_column("amount") is None
        assert _classify_column("city") is None
        assert _classify_column("id") is None
        assert _classify_column("status") is None

    def test_custom_pattern(self):
        assert _classify_column("employee_id", ["employee_id"]) == "custom"
        assert _classify_column("bank_account", ["bank_account"]) == "custom"
        assert _classify_column("user_name", ["employee_id"]) is None


class TestMaskRows:
    def test_phone_masking(self):
        rows = [{"name": "Alice", "phone": "13812345678"}]
        schema = [{"name": "name"}, {"name": "phone"}]
        masked, cols = mask_rows(rows, schema)
        assert masked[0]["phone"] == "138****5678"
        assert masked[0]["name"] == "Alice"
        assert cols == ["phone"]

    def test_email_masking(self):
        rows = [{"email": "john@example.com"}]
        schema = [{"name": "email"}]
        masked, cols = mask_rows(rows, schema)
        assert masked[0]["email"] == "j***@example.com"
        assert cols == ["email"]

    def test_password_masking(self):
        rows = [{"password": "supersecret123"}]
        schema = [{"name": "password"}]
        masked, cols = mask_rows(rows, schema)
        assert masked[0]["password"] == "******"

    def test_id_card_masking(self):
        rows = [{"id_card": "110101199001011234"}]
        schema = [{"name": "id_card"}]
        masked, cols = mask_rows(rows, schema)
        assert masked[0]["id_card"] == "110***********1234"

    def test_none_value_preserved(self):
        rows = [{"phone": None, "name": "Bob"}]
        schema = [{"name": "phone"}, {"name": "name"}]
        masked, cols = mask_rows(rows, schema)
        assert masked[0]["phone"] is None
        assert cols == ["phone"]

    def test_empty_rows(self):
        rows = []
        schema = [{"name": "phone"}]
        masked, cols = mask_rows(rows, schema)
        assert masked == []
        assert cols == []

    def test_no_sensitive_columns(self):
        rows = [{"id": 1, "name": "Alice", "city": "NYC"}]
        schema = [{"name": "id"}, {"name": "name"}, {"name": "city"}]
        masked, cols = mask_rows(rows, schema)
        assert masked == rows
        assert cols == []

    def test_extra_sensitive_columns(self):
        rows = [{"employee_id": "E12345", "name": "Alice"}]
        schema = [{"name": "employee_id"}, {"name": "name"}]
        masked, cols = mask_rows(rows, schema, extra_sensitive_columns=["employee_id"])
        assert masked[0]["employee_id"] == "***"
        assert cols == ["employee_id"]

    def test_multiple_sensitive_columns(self):
        rows = [{"phone": "13800001111", "email": "a@b.com", "name": "X"}]
        schema = [{"name": "phone"}, {"name": "email"}, {"name": "name"}]
        masked, cols = mask_rows(rows, schema)
        assert masked[0]["phone"] == "138****1111"
        assert masked[0]["email"] == "a***@b.com"
        assert masked[0]["name"] == "X"
        assert cols == ["email", "phone"]  # sorted

    def test_original_rows_not_mutated(self):
        rows = [{"phone": "13812345678"}]
        schema = [{"name": "phone"}]
        masked, _ = mask_rows(rows, schema)
        assert rows[0]["phone"] == "13812345678"
        assert masked[0]["phone"] == "138****5678"

    def test_short_phone(self):
        rows = [{"phone": "12345"}]
        schema = [{"name": "phone"}]
        masked, _ = mask_rows(rows, schema)
        assert masked[0]["phone"] == "***"

    def test_schema_fallback_to_row_keys(self):
        """When schema is empty, use row keys."""
        rows = [{"phone": "13812345678", "name": "Alice"}]
        masked, cols = mask_rows(rows, [])
        assert masked[0]["phone"] == "138****5678"
        assert cols == ["phone"]
