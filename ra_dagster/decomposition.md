# Decomposition

Generalize structural decomposition like this:

Any model output can be written as
**Y = f(X, θ)**
where:

- **X** = data (features, population)
- **θ** = model structure (coeffs, hyperparams, mapping logic)
- **f** = how the model turns data into results

You have two states:

- **baseline**: X₀, θ₀
- **new**: X₁, θ₁

Total change:

```
ΔY = f(X₁, θ₁) − f(X₀, θ₀)
```

Break the change into effects by **holding one thing constant at a time**:

**Data effect**

```
Δ_data = f(X₁, θ₀) − f(X₀, θ₀)
```

**Model/coeff effect**

```
Δ_model = f(X₀, θ₁) − f(X₀, θ₀)
```

**Interaction effect**

```
Δ_inter = f(X₁, θ₁) − f(X₁, θ₀) − Δ_model
```

These always add up:

```
ΔY = Δ_data + Δ_model + Δ_inter
```
