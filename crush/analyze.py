# -*- mode: python; coding: utf-8 -*-
#
# Copyright (C) 2017 <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
from __future__ import division

import argparse
import collections
import copy
import logging
import textwrap
import pandas as pd
import numpy as np

from crush import Crush

log = logging.getLogger(__name__)


class Analyze(object):

    def __init__(self, args, main):
        self.args = args
        self.main = main

    @staticmethod
    def get_parser():
        parser = argparse.ArgumentParser(
            add_help=False,
            conflict_handler='resolve',
        )
        replication_count = 3
        parser.add_argument(
            '--replication-count',
            help=('number of devices to map (default: %d)' % replication_count),
            type=int,
            default=replication_count)
        parser.add_argument(
            '--rule',
            help='the name of rule')
        parser.add_argument(
            '--type',
            help='override the type of bucket shown in the report')
        parser.add_argument(
            '--choose-args',
            help='modify the weights')
        parser.add_argument(
            '--crushmap',
            help='path to the crushmap file')
        parser.add_argument(
            '-w', '--weights',
            help='path to the weights file')
        values_count = 100000
        parser.add_argument(
            '--values-count',
            help='repeat mapping (default: %d)' % values_count,
            type=int,
            default=values_count)
        return parser

    @staticmethod
    def set_parser(subparsers, arguments):
        parser = Analyze.get_parser()
        arguments(parser)
        subparsers.add_parser(
            'analyze',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            Analyze a crushmap rule

            The first step shows if the distribution run by a
            simulation is different from what is expected with the
            weights assigned to each item in crushmap.

            Map a number of objects (--values-count) to devices (three
            by default or --replication-count if specified) using a
            crush rule (--rule) from a given crushmap (--crushmap) and
            display a report comparing the expected and the actual
            object distribution.

            The format of the crushmap file specified with --crushmap
            can either be:

            - a JSON representation of a crushmap as documented in the
              Crush.parse_crushmap() method

            - a Ceph binary, text or JSON crushmap compatible with
              Luminuous and below

            The --type argument changes the item type displayed in the
            report. For instance --type device shows the individual
            OSDs and --type host shows the machines that contain
            them. If --type is not specified, it defaults to the
            "type" argument of the first "choose*" step of the rule
            selected by --rule.

            The first item in the report will be the first to become
            full. For instance if the report starts with:

                    ~id~  ~weight~  ~objects~  ~over/under used %~
            ~name~
            g9       -22  2.29             85                10.40

            it means that the bucket g9 with id -22 and weight 2.29
            will be the first bucket of its type to become full. The
            actual usage of the host will be 10.4% over the expected
            usage, i.e. if the g9 host is expected to be 70%
            full, it will actually be 80.40% full.

            The ~over/under used %~ is the variation between the
            expected item usage and the actual item usage. If it is
            positive the item is overused, if it is negative the item
            is underused.

            The second step shows the worst case scenario if a bucket
            in the failure domain is removed from the crushmap. The
            failure domain is the type argument of the crush rule.
            For instance in:

                ["chooseleaf", "firstn", 0, "type", "host"]

            the failure domain is the host. If there are four hosts in
            the crushmap, named host1, host2, etc. a simulation will
            be run with a crushmap in which only host1 was
            removed. Another simulation will be run with a crushmap
            where host2 was removed etc. The result of all simulations
            are aggregated together.

            The worst case scenario for each item type is when the
            overfull percentage is higher. It is displayed as follows:

                     ~over used %~
            ~type~
            device          25.55
            host            22.45

            If a host fail, the worst case scenario is that a device
            will be 25.55% overfull or a host will be 22.45% overfull.

            """),
            epilog=textwrap.dedent("""
            Examples:

            Display the first host that will become full.

            $ crush analyze --values-count 100 --rule data \\
                            --crushmap tests/sample-crushmap.json
                    ~id~  ~weight~  ~objects~  ~over/under used %~
            ~name~
            host2     -4       1.0         70                  5.0
            host0     -2       1.0         65                 -2.5
            host1     -3       1.0         65                 -2.5

            Display the first device that will become full.

            $ crush analyze --values-count 100 --rule data \\
                            --type device \\
                            --crushmap tests/sample-crushmap.json
                     ~id~  ~weight~  ~objects~  ~over/under used %~
            ~name~
            device0     0       1.0         28                26.00
            device4     4       1.0         24                 8.00
            device5     5       2.0         46                 3.50
            device3     3       2.0         44                -1.00
            device2     2       1.0         21                -5.50
            device1     1       2.0         37               -16.75
            """),
            help='Analyze crushmaps',
            parents=[parser],
        ).set_defaults(
            func=Analyze,
        )

    @staticmethod
    def collect_dataframe(crush, child):
        paths = crush.collect_paths([child], collections.OrderedDict())
        #
        # verify all paths have bucket types in the same order in the hierarchy
        # i.e. always rack->host->device and not host->rack->device sometimes
        #
        key2pos = {}
        pos2key = {}
        for path in paths:
            keys = list(path.keys())
            for i in range(len(keys)):
                key = keys[i]
                if key in key2pos:
                    assert key2pos[key] == i
                else:
                    key2pos[key] = i
                    pos2key[i] = key
        columns = []
        for pos in sorted(pos2key.keys()):
            columns.append(pos2key[pos])
        rows = []
        for path in paths:
            row = []
            for column in columns:
                element = path.get(column, np.nan)
                row.append(element)
                if element is not np.nan:
                    item_name = element
            item = crush.get_item_by_name(item_name)
            rows.append([item['id'],
                         item_name,
                         item.get('weight', 1.0),
                         item.get('type', 'device')] + row)
        d = pd.DataFrame(rows, columns=['~id~', '~name~', '~weight~', '~type~'] + columns)
        return d.set_index('~name~')

    @staticmethod
    def collect_cropped_weights(d, replication_count, failure_domain):
        d['~overweight~'] = False
        d['~cropped weight~'] = d['~weight~'].copy()
        d['~cropped %~'] = 0.0
        for type in (failure_domain, 'device'):
            if len(d.loc[d['~type~'] == type]) == 0:
                continue
            w = d.loc[d['~type~'] == type].copy()
            tw = w['~weight~'].sum()
            w['~overweight~'] = w['~weight~'].apply(lambda w: w > tw / replication_count)
            overweight_count = len(w.loc[w['~overweight~']])
            if overweight_count > 0:
                tw_not_overweight = w.loc[~w['~overweight~'], ['~weight~']].sum()['~weight~']
                assert replication_count > overweight_count
                cropped_weight = tw_not_overweight / (replication_count - overweight_count)
                w.loc[w['~overweight~'], ['~cropped weight~']] = cropped_weight
                w['~cropped %~'] = (1.0 - w['~cropped weight~'] / w['~weight~']) * 100
            d.loc[d['~type~'] == type] = w
        return d

    @staticmethod
    def collect_nweight(d):
        d['~nweight~'] = 0.0
        for type in d['~type~'].unique():
            w = d.loc[d['~type~'] == type].copy()
            tw = w['~cropped weight~'].sum()
            w['~nweight~'] = w['~cropped weight~'].apply(lambda w: w / float(tw))
            d.loc[d['~type~'] == type] = w
        return d

    @staticmethod
    def collect_expected_objects(d, total):
        d['~expected~'] = 0
        for type in d['~type~'].unique():
            e = d.loc[d['~type~'] == type].copy()
            e['~expected~'] = e['~nweight~'].apply(lambda w: total * w).astype(int)
            remainder = total - e['~expected~'].sum()
            if remainder > 0:
                rounding = e['~expected~'].copy()
                rounding[:remainder] += 1
                e['~expected~'] = rounding
                assert total - e['~expected~'].sum() == 0
            d.loc[d['~type~'] == type] = e
        return d

    @staticmethod
    def collect_usage(d, total_objects):
        capacity = d['~nweight~'] * float(total_objects)
        d['~over/under used %~'] = (d['~objects~'] / capacity - 1.0) * 100 - d['~cropped %~']
        return d

    def run_simulation(self, c, root_name, failure_domain):
        if self.args.weights:
            with open(self.args.weights) as f_weights:
                weights = c.parse_weights_file(f_weights)
        else:
            weights = None

        values = self.main.hook_create_values()
        replication_count = self.args.replication_count
        total_objects = replication_count * len(values)

        root = c.find_bucket(root_name)
        log.debug("root = " + str(root))
        d = Analyze.collect_dataframe(c, root)
        d = Analyze.collect_cropped_weights(d, replication_count, failure_domain)
        d = Analyze.collect_nweight(d)
        d = Analyze.collect_expected_objects(d, total_objects)

        rule = self.args.rule
        device2count = collections.defaultdict(lambda: 0)
        for (name, value) in values.items():
            m = c.map(rule, value, replication_count, weights, choose_args=self.args.choose_args)
            log.debug("{} == {} mapped to {}".format(name, value, m))
            assert len(m) == replication_count
            for device in m:
                device2count[device] += 1

        item2path = c.collect_item2path([root])
        log.debug("item2path = " + str(item2path))
        d['~objects~'] = 0
        for (device, count) in device2count.items():
            for item in item2path[device]:
                d.at[item, '~objects~'] += count

        return Analyze.collect_usage(d, total_objects)

    def analyze_failures(self, c, take, failure_domain):
        if failure_domain == 0:  # failure domain == device is a border case
            return None
        root = c.find_bucket(take)
        worst = pd.DataFrame()
        for may_fail in c.collect_buckets_by_type([root], failure_domain):
            f = Crush(verbose=self.args.verbose,
                      backward_compatibility=self.args.backward_compatibility)
            f.crushmap = copy.deepcopy(c.get_crushmap())
            root = f.find_bucket(take)
            f.filter(lambda x: x.get('name') != may_fail.get('name'), root)
            f.parse(f.crushmap)
            a = self.run_simulation(f, take, failure_domain)
            a['~over used %~'] = a['~over/under used %~']
            a = a[['~type~', '~over used %~']]
            worst = pd.concat([worst, a]).groupby(['~type~']).max().reset_index()
        return worst.set_index('~type~')

    def _format_report(self, d, type):
        s = (d['~type~'] == type) & (d['~weight~'] > 0)
        a = d.loc[s, ['~id~', '~weight~', '~objects~', '~over/under used %~']]
        return str(a.sort_values(by='~over/under used %~', ascending=False))

    def analyze(self):
        c = Crush(verbose=self.args.verbose,
                  backward_compatibility=self.args.backward_compatibility)
        c.parse(self.args.crushmap)
        (take, failure_domain) = c.rule_get_take_failure_domain(self.args.rule)
        if self.args.type:
            type = self.args.type
        else:
            type = failure_domain
        pd.set_option('precision', 2)

        d = self.run_simulation(c, take, failure_domain)
        out = ""
        out += self._format_report(d, type)
        out += "\n\nWorst case scenario if a " + str(failure_domain) + " fails:\n\n"
        worst = self.analyze_failures(c, take, failure_domain)
        if worst is not None:
            out += str(worst)
        if d['~overweight~'].any():
            out += "\n\nThe following are overweight and should be cropped:\n\n"
            out += str(d.loc[d['~overweight~'],
                             ['~id~', '~weight~', '~cropped weight~', '~cropped %~']])
        return out

    def run(self):
        if not self.args.crushmap:
            raise Exception("missing --crushmap")
        return self.analyze()
