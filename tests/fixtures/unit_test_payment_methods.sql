unit_tests:
  - name: test_dim_payment_methods_logic
    model: dim_payment_methods

    given:
      - input: ref('stg_payment_methods')
        rows:
          - { method_id: 1, method_name: "credit_card" }
          - { method_id: 2, method_name: "debit_card" }
          - { method_id: 3, method_name: "tng" }
          - { method_id: 4, method_name: "grabpay" }
          - { method_id: 5, method_name: "cash" }
          - { method_id: 6, method_name: "random_other" }

    expect:
      rows:
        - { method_id: 1, method_name: "credit_card", payment_method: "Credit Card" }
        - { method_id: 2, method_name: "debit_card",  payment_method: "Debit Card" }
        - { method_id: 3, method_name: "tng",         payment_method: "TNG" }
        - { method_id: 4, method_name: "grabpay",     payment_method: "Grab Pay" }
        - { method_id: 5, method_name: "cash",        payment_method: "Cash" }
        - { method_id: 6, method_name: "random_other", payment_method: "Cash" }
