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
from rxbp.indexed.indexedflowable import IndexedFlowable

subject = Subject()


fdict = {
    't_main': rxbp.indexed.from_flowable(source=rxbp.from_rx(subject), base='t_main').share(),
    't_1': rxbp.indexed.range(0, 100, base='t_1').pipe(
        rxbp.op.map(lambda i: 10 * i/100 * 0.9 + 2.1),
    ).share(),
    't_2': rxbp.indexed.range(0, 100, base='t_2').pipe(
        rxbp.op.map(lambda i: 10 * i/100 * 1.05 + 0.9),
    ).share(),
}


def time_value_flowables(fdict: Dict[str, IndexedFlowable]):
    return {
        'v_main': fdict['t_main'].pipe(
            rxbp.op.map(lambda t: math.cos(t * 2 * math.pi - math.pi / 8)),
        ),
        'v_1': fdict['t_1'].pipe(
            rxbp.op.map(lambda t: math.cos(t * 2 * math.pi - math.pi / 8)),
        ),
        'v_2': fdict['t_2'].pipe(
            rxbp.op.map(lambda t: math.cos(t * 2 * math.pi - math.pi / 8)),
        ),
        **fdict,
    }


def interpolate_value(t_pair, v_pair, t):
    (t1, t2) = t_pair
    (v1, v2) = v_pair

    return v1 + (t - t1) / (t2 - t1) * (v2 - v1)


def sync_t1_to_tmain(fdict: Dict[str, IndexedFlowable]):
    t_1_base_main = fdict['t_1'].pipe(
        rxbp.op.pairwise(),
        rxbp.op.controlled_zip(
            right=fdict['t_main'],
            request_left=lambda l, r: l[1] <= r,
            request_right=lambda l, r: r < l[1],
            match_func=lambda l, r: l[0] <= r < l[1],
        ),
    )

    return {
        't_1_base_main': t_1_base_main,
        **fdict
    }


def sync_t2_to_tmain(fdict: Dict[str, IndexedFlowable]):
    t_2_base_main = fdict['t_2'].pipe(
        rxbp.op.pairwise(),
        rxbp.op.controlled_zip(
            right=fdict['t_main'],
            request_left=lambda l, r: l[1] <= r,
            request_right=lambda l, r: r < l[1],
            match_func=lambda l, r: l[0] <= r < l[1],
        ),
    )

    return {
        't_2_base_main': t_2_base_main,
        **fdict
    }


def sync_t1_and_t2(fdict: Dict[str, IndexedFlowable]):
    t_sync = fdict['t_1_base_main'].pipe(
        rxbp.indexed.op.match(
            fdict['t_2_base_main'],
        ),
    )

    return {
        't_sync': t_sync.pipe(rxbp.op.map(lambda t: t[0][1])),
        't_1_sync': t_sync.pipe(rxbp.op.map(lambda t: t[0])),
        't_2_sync': t_sync.pipe(rxbp.op.map(lambda t: t[1])),
        **fdict,
    }


def interpolate_v1(fdict: Dict[str, IndexedFlowable]):
    v_1_sync = fdict['v_1'].pipe(
        rxbp.op.pairwise(),
        rxbp.indexed.op.match(
            fdict['t_1_sync'],
        ),
        rxbp.op.map(lambda t: interpolate_value(t_pair=t[1][0], v_pair=t[0], t=t[1][1]))
    )

    return {
        'v_1_sync': v_1_sync,
        **fdict,
    }


def interpolate_v2(fdict: Dict[str, IndexedFlowable]):
    v_2_sync = fdict['v_2'].pipe(
        rxbp.op.pairwise(),
        rxbp.indexed.op.match(
            fdict['t_2_sync'],
        ),
        rxbp.op.map(lambda t: interpolate_value(t_pair=t[1][0], v_pair=t[0], t=t[1][1]))
    )

    return {
        'v_2_sync': v_2_sync,
        **fdict,
    }


def sel_flow(fdict: Dict[str, IndexedFlowable]):
    output = fdict['t_sync'].pipe(
        rxbp.op.zip(
            fdict['v_1_sync'],
            fdict['v_2_sync'],
        ),
    )

    return output


result = []


rxbp.multicast.return_value(fdict).pipe(
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

