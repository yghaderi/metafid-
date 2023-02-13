import numpy as np


def gbm(s_0: float, N: int, T: int, n_sims: int, mu: float, sigma: float, random_seed: int = 42):
    """
    Geometric Brownian Motion (GBM)
    :return:
    """
    np.random.seed(random_seed)
    dt = N / T
    dw = np.random.normal(scale=np.sqrt(dt), size=(n_sims, N))
    w = np.cumsum(dw, axis=1)

    time_step = np.linspace(dt, T, N)
    time_steps = np.broadcast_to(time_step, (n_sims, N))

    S_t = np.insert((s_0 * np.exp((mu - pow(sigma, 2) / 2) * time_steps + sigma * w)),
                    0,
                    s_0,
                    axis=1,
                    )
    return S_t