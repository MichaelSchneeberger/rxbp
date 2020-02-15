"""
This example uses the match operator to match three records with different
sampling instances.
"""

import math
from typing import Dict

import rx
from matplotlib import pyplot
from rx import operators as rxop
from rx.subject import Subject

import rxbp
from rxbp.flowable import Flowable

subject = Subject()


fdict = {
    't_main': rxbp.from_rx(subject, base='t_main'),
    't_1': rxbp.range(0, 100, base='t_1').map(lambda i: 10 * i/100 * 0.9 + 2.1),
    't_2': rxbp.range(0, 100, base='t_2').map(lambda i: 10 * i/100 * 1.05 + 0.9),
}


def time_value_flowables(fdict: Dict[str, Flowable]):
    return {
        'v_main': fdict['t_main'].map(lambda t: math.cos(t * 2 * math.pi - math.pi / 8)).share(),
        'v_1': fdict['t_1'].map(lambda t: math.cos(t * 2 * math.pi - math.pi / 8)).share(),
        'v_2': fdict['t_2'].map(lambda t: math.cos(t * 2 * math.pi - math.pi / 8)).share(),
        **fdict,
    }


def interpolate_value(t_pair, v_pair, t):
    (t1, t2) = t_pair
    (v1, v2) = v_pair

    return v1 + (t - t1) / (t2 - t1) * (v2 - v1)


def sync_t1_to_tmain(fdict: Dict[str, Flowable]):
    t_1_base_main = fdict['t_1'].pairwise().pipe(
        rxbp.op.controlled_zip(
            right=fdict['t_main'],
            request_left=lambda l, r: l[1] <= r,
            request_right=lambda l, r: r < l[1],
            match_func=lambda l, r: l[0] <= r < l[1],
        ),
    ).share()

    return {
        't_1_base_main': t_1_base_main,
        **fdict
    }


def sync_t2_to_tmain(fdict: Dict[str, Flowable]):
    t_2_base_main = fdict['t_2'].pairwise().pipe(
        rxbp.op.controlled_zip(
            right=fdict['t_main'],
            request_left=lambda l, r: l[1] <= r,
            request_right=lambda l, r: r < l[1],
            match_func=lambda l, r: l[0] <= r < l[1],
        ),
    ).share()

    return {
        't_2_base_main': t_2_base_main,
        **fdict
    }


def sync_t1_and_t2(fdict: Dict[str, Flowable]):
    t_sync = fdict['t_1_base_main'].pipe(
        rxbp.op.match(
            fdict['t_2_base_main'],
        ),
    ).share()

    return {
        't_sync': t_sync.map(lambda t: t[0][1]).share(),
        't_1_sync': t_sync.map(lambda t: t[0]).share(),
        't_2_sync': t_sync.map(lambda t: t[1]).share(),
        **fdict,
    }


def interpolate_v1(fdict: Dict[str, Flowable]):
    v_1_sync = fdict['v_1'].pairwise().pipe(
        rxbp.op.match(
            fdict['t_1_sync'],
        ),
        rxbp.op.map(lambda t: interpolate_value(t_pair=t[1][0], v_pair=t[0], t=t[1][1]))
    ).share()

    return {
        'v_1_sync': v_1_sync,
        **fdict,
    }


def interpolate_v2(fdict: Dict[str, Flowable]):
    v_2_sync = fdict['v_2'].pairwise().pipe(
        rxbp.op.match(
            fdict['t_2_sync'],
        ),
        rxbp.op.map(lambda t: interpolate_value(t_pair=t[1][0], v_pair=t[0], t=t[1][1]))
    ).share()

    return {
        'v_2_sync': v_2_sync,
        **fdict,
    }


def sel_flow(fdict: Dict[str, Flowable]):
    output = fdict['t_sync'].pipe(
        rxbp.op.zip(
            fdict['v_1_sync'],
            fdict['v_2_sync'],
        ),
    )

    return output


result = []


rxbp.multicast.return_flowable(fdict).pipe(
    rxbp.multicast.op.map(time_value_flowables),
    rxbp.multicast.op.map(sync_t1_to_tmain),
    rxbp.multicast.op.map(sync_t2_to_tmain),
    rxbp.multicast.op.map(sync_t1_and_t2),
    rxbp.multicast.op.map(interpolate_v1),
    rxbp.multicast.op.map(interpolate_v2),
    rxbp.multicast.op.map(sel_flow),
).to_flowable().subscribe(
    on_next=lambda v: result.append(v),
    on_completed=lambda: print('completed')
)


rx.range(0, 1000).pipe(
    rxop.map(lambda i: 10 * i/1000 * 1.01 + 1.4),
).subscribe(subject)

t, v1, v2 = list(zip(*result))

pyplot.plot(t, v1)
pyplot.plot(t, v2)
pyplot.show()

