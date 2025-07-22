import numpy as np

def get_all_files(folder_path):
    files = sorted(
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    )
    return files


def module_temperature(T_air, G, T_NOCT=48, T_0=20, G_0=800):
    return T_air + (T_NOCT - T_0) * (G / G_0)


def relative_efficiency(G, T_air, G_STC=1000, T_STC=25):
    # Constants from the paper
    a = 1.20e-3
    b = -4.60e-3
    c1 = 0.033
    c2 = -0.0092
    G_norm = G / G_STC
    T_mod = module_temperature(T_air, G)
    delta_T = T_mod - T_STC
    h_rel = (1 + a * delta_T) * (
        1 + c1 * np.log(G_norm) + c2 * (np.log(G_norm)) ** 2 + b * delta_T
    )
    return h_rel


def calc_capacity_factor(h_rel, G, G_STC=1000):
    return h_rel * (G / G_STC)
