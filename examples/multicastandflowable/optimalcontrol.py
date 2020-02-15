import math
from typing import Tuple, Dict, List

import cvxpy as cp
import numpy as np
import rxbp
from matplotlib import pyplot
from numpy import polymul, polyadd
from rxbp.flowable import Flowable
from rxbp.multicast.multicast import MultiCast
from scipy import signal

# hyper parameters
# ----------------

# simulation parameters
t_sim = 10.02e-1            # totale simulation time in [s]
dt_sim = 8e-4               # simulation sample time in [s]

# 1 phase rail grid parameters
f0 = 16.7                   # nominal frequency in [1/s]
x_transformer = 0.18        # transformer reactive impedance in [Ohm]
r_transformer = 0.01        # transformer active impedance in [Ohm]
x_ind_grid = 0.3            # grid reactive impedance of inductor in [Ohm]
r_grid = 0.01               # grid resistive impedance in [Ohm]
x_cap_grid = 0.1            # grid reactive impedance of capacitor in [Ohm]

# Hankel matrix
n_col = 100                 # number of column of Hankel matrix
n_predict = 20              # prediction horizon
n_order = 5                 # order of system to be estimated

# Some pre-computations
# --------------------

n_samples = int(t_sim / dt_sim)                 # number of simulation iteration
w0 = 2 * math.pi * f0                           # angular frequency in [rad/s]
l_transformer = x_transformer / w0              # transformer inductance in [H]
l_grid = x_ind_grid / w0                        # grid inductance in [H]
c_grid = 1 / (x_cap_grid * w0)                  # grid capacitance in [F]
n_meas = n_col + (n_order + n_predict - 1)      # number of measurements required to form the Hankel matrix

# define impedances as transfer function in s-domain
z_transformer = signal.TransferFunction([l_transformer, r_transformer], [1])
z_cap_grid = signal.TransferFunction([1], [c_grid, 0])
z_ind_grid = signal.TransferFunction([l_grid, r_grid], [1])

def tf_sum(tf1, tf2):
    num = polyadd(polymul(tf1.num, tf2.den), polymul(tf1.den, tf2.num))
    den = polymul(tf1.den, tf2.den)
    return signal.TransferFunction(num, den)


def tf_mul(tf1, tf2):
    num = polymul(tf1.num, tf2.num)
    den = polymul(tf1.den, tf2.den)
    return signal.TransferFunction(num, den)


def tf_div(tf1, tf2):
    num = polymul(tf1.num, tf2.den)
    den = polymul(tf1.den, tf2.num)
    return signal.TransferFunction(num, den)

# generate state space model with total impedance seen by the converter
z_grid = tf_div(tf_mul(z_ind_grid, z_cap_grid), tf_sum(z_ind_grid, z_cap_grid))
y_total = tf_div(signal.TransferFunction([1], [1]), tf_sum(z_transformer, z_grid))
dss = y_total.to_ss().to_discrete(dt_sim)

# Helper classes and functions
# ----------------------------


class Base:
    """ The Base object mimics a dictionary but add two fields 'time' and 'phi'.
    """

    def __init__(self, time, phi, signals: Dict[str, Flowable] = None):
        self.time = time
        self.phi = phi

        self.signals = signals or {}

    def copy(self, signals: Dict[str, Flowable]):
        return Base(time=self.time, phi=self.phi, signals={**self.signals, **signals})

    def __setitem__(self, key, value):
        self.signals[key] = value

    def __getitem__(self, item):
        return self.signals[item]


def create_hankel_matrix(u_last: List[float], y_last: List[float], n_predict: int, n_order: int):
    """ Create Hankel matrix from measured output values y and input values u

    A1 = [u1    u2 u3 u4 ... u_L
          u2    u3 u4
          u3    u4
          u4
          ...
          u_o+p              u_o+p+L-1

          y1
          ...
          y_o                y_o+L-1
         ]

    C1 = [y_o+1  ...        y_o+L
          ...
          y_o+p  ...        y_o+p+L-1
         ]

    m = n_order + n_predict + n_order
    L = n_meas - n_order - n_predict + 1
    """

    assert len(u_last) == len(y_last)

    m = n_order + n_predict
    n = len(u_last) - m + 1

    def gen_u_columns():
        for i in range(n):
            yield np.array([u_last[i:m + i]]).T

    def gen_y_columns():
        for i in range(n):
            yield np.array([y_last[i:m + i]]).T

    AU = np.concatenate(list(gen_u_columns()), axis=1)
    AY = np.concatenate(list(gen_y_columns()), axis=1)

    # set of rows 1
    A1 = np.concatenate([
        AU[:n_order, :],
        AY[:n_order, :],
        AU[n_order:, :],
    ])

    # set of rows 2: input to be predicted
    A2 = np.concatenate([
        np.zeros((n_order, n_predict)),     # previous input values
        np.zeros((n_order, n_predict)),     # previous output values
        -np.eye(n_predict, n_predict),  # input to be predicted
    ])

    # set of rows 3: error from y_prev to y_prev'
    A3 = np.concatenate([
        np.zeros((n_order, n_order)),       # previous input values
        -np.eye(n_order, n_order),          # previous output values
        np.zeros((n_predict, n_order)),     # input to be predicted
    ])

    A = np.concatenate([A1, A2, A3], axis=1)

    b = np.concatenate([
        u_last[-n_order:],                  # previous input values
        y_last[-n_order:],                  # previous output values
        np.zeros(n_predict),                # input to be predicted
    ])

    C = np.concatenate([
        AY[n_order:, :],
        np.zeros((n_predict, n_predict + n_order)),
    ], axis=1)

    return A, b, C


def solve_opt_prob(A, b, C, y_ref, u_last):
    """ Solve optimization problem via cvxpy

    min.    sum(squares(C*x - y_ref)) + p1 * sum(squares(s1 * u[0])) + p2 * sum(squares(s2 * sigma))
                                      + p3 * sum(squares(s3 * sigma[-1]))
    s.t.    A*x == b
            1.3 <= u <= 1.3
    where   x = [[g], [u], [sigma]]
    """

    n1 = n_col
    n2 = n_predict
    n3 = n_order

    # Construct the problem.
    x = cp.Variable(n1 + n2 + n3)
    objective = cp.Minimize(
        1 * cp.sum_squares(C @ x - y_ref)
        + 0.001 * cp.sum_squares(x[n1:n1 + 1] - u_last)  # rate of change
        + 0.0001 * cp.sum_squares(x[n1 + n2:n1 + n2 + n3])  # match previous output
        + 0.001 * cp.sum_squares(x[n1 + n2 + n3 - 1])  # match previous output
    )
    constraints = [A @ x == b, cp.abs(x[n1:n1 + n2]) <= 1.3]
    prob = cp.Problem(objective, constraints)

    result = prob.solve()

    return x.value, result


# Create Simulation
# -----------------


def create_time(fdict):
    """ generate time from index
    """

    time = fdict['idx'].pipe(
        rxbp.op.map(lambda idx: idx*dt_sim),
    ).share()

    return {**fdict, 'time': time}


def create_phi(fdict):
    """ generate phi from time
    """

    time = fdict['time'].pipe(
        rxbp.op.map(lambda t: t*w0),
    ).share()

    return {**fdict, 'phi': time}


def loop_previous_state(looped_multicast: MultiCast):
    #     """ the converter current `y` depends on
    #     - the previous converter voltage `u`,
    #     - the previous state `x`, and
    #     - the previous converter current `y`.
    #
    #     time, phi
    #         |
    #     merge_input_and_state <------------------
    #         |                                   |
    #     collect_last_inputs_and_outputs         |
    #         |                                   |
    #     create_uref                             |
    #         |                                 u, x, y
    #     create_next_state                       |
    #         |                                   |
    #         * -----------------------------------
    #         |
    #     time, phi, u, y_ref, y, cost_val
    #     """

    def map_flowables(fdict: Dict[str, Flowable]):

        def acculate(acc: Tuple[List[float], List[float]], t2: Tuple[float, float]):
            last_u, last_y = acc
            curr_u, curr_y = t2

            return last_u[1:] + [curr_u], last_y[1:] + [curr_y]

        last = rxbp.zip(
            fdict['u'],     # previous u
            fdict['y'],     # previous y
        ).pipe(
            rxbp.op.scan(acculate, ([0.0] * n_meas, [0.0] * n_meas))
        ).share()

        def calculate_u(
                time: float,
                phi: float,
                last: Tuple[List[float], List[float]]
        ):
            y_ref = np.array([34.0 * math.sin(phi + w0 * dt_sim * n - 0.365 * math.pi) for n in range(n_predict)])

            if 5e-1 < time:
                A, b, C = create_hankel_matrix(u_last=last[0], y_last=last[1], n_predict=n_predict, n_order=n_order)
                u, cost_val = solve_opt_prob(A, b, C, y_ref, last[0][-1])
            else:
                u = None
                cost_val = 0

            if u is None:
                u_sel = 1.0 * math.sin(phi) + 0.02*math.cos(phi*7+math.pi/7)
            else:
                u_sel = u[n_col]

            return u_sel, y_ref[0], cost_val

        obs = rxbp.zip(
            fdict['time'],
            fdict['phi'],
            last,
        ).pipe(
            rxbp.op.map(lambda v: calculate_u(*v)),
        ).share()

        u = obs.map(lambda t: t[0])
        y_ref = obs.map(lambda t: t[1])
        cost_val = obs.map(lambda t: t[2])

        def simulate_epoche(u: float, prev_x: List):
            next_x0 = np.matmul(dss.A, prev_x) + np.matmul(dss.B, [u])
            next_y = np.matmul(dss.C, next_x0).tolist()[0]
            return next_x0, next_y

        obs = u.pipe(
            rxbp.op.zip(fdict['x']),        # previous x
            rxbp.op.map(lambda v: simulate_epoche(*v)),
        )

        state = obs.map(lambda t2: t2[0])
        y = obs.map(lambda t2: t2[1])

        return {
            **fdict,
            'u': u,
            'x': state,
            'y': y,
            'y_ref': y_ref,
            'cost_val': cost_val,
        }

    return looped_multicast.pipe(
        rxbp.multicast.op.map(map_flowables),
    )


simulation = rxbp.multicast.return_flowable({'idx': rxbp.range(n_samples, batch_size=100)}).pipe(
    rxbp.multicast.op.map(create_time),
    rxbp.multicast.op.map(create_phi),
    rxbp.multicast.op.loop_flowables(
        func=loop_previous_state,
        initial={
            'u': 0,
            'x': [0, 0, 0, 0],
            'y': 0,
        },
    )
)

# run Simulation
# --------------

output = simulation.to_flowable().run()[0]


# plot output data
# ----------------

fig = pyplot.figure(1)
axs = fig.subplots(3, 1)
axs[0].set_ylabel('converter voltage')
axs[0].plot(output['time'], output['u'])
axs[1].set_ylabel('converter current')
axs[1].plot(output['time'], output['y'])
axs[1].plot(output['time'], output['y_ref'])
axs[2].set_ylabel('cost')
axs[2].plot(output['time'], output['cost_val'])
axs[2].set_xlabel('t [s]')
pyplot.show()
