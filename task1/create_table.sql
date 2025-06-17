CREATE TABLE transactions_v2 (
  msno Utf8,
  payment_method_id Int32,
  payment_plan_days Int32,
  plan_list_price Int32,
  actual_amount_paid Int32,
  is_auto_renew Bool,
  transaction_date Uint32,
  membership_expire_date Uint32,
  is_cancel Bool,
  PRIMARY KEY (msno, transaction_date)
);
