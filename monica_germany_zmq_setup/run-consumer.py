#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

# print sys.path

from collections import defaultdict  # , OrderedDict
import csv
# from datetime import datetime
from netCDF4 import Dataset
# import types
import json
# import gc
import numpy as np
import os
# import sqlite3
import sys
import time
# import timeit
import zmq

import monica_io3
# import soil_io3
# print "path to monica_io: ", monica_io.__file__
import monica_run_lib as Mrunlib

PATHS = {
    "mbm-local-remote": {
        "path-to-output-dir": "out/",
        "path-to-csv-output-dir": "csv-out/"
    },
    "remoteConsumer-remoteMonica": {
        "path-to-output-dir": "/out/out/",
        "path-to-csv-output-dir": "/out/csv-out/"
    }
}
DEFAULT_HOST = "localhost" # "login01.cluster.zalf.de"  # "localhost"
DEFAULT_PORT = "7777"

def write_output_to_netcdfs(row, col, msg_data, npas):
    for data in msg_data:
        results = data.get("results", [])
        for i, npa in enumerate(npas):
            npa[:, row, col] = np.fromiter(map(lambda v: v*100.0, results[i]), dtype="f8")


def run_consumer(leave_after_finished_run=True, server={"server": None, "port": None}):
    "collect data from workers"

    config = {
        "mode": "mbm-local-remote",
        "port": server["port"] if server["port"] else DEFAULT_PORT,
        "server": server["server"] if server["server"] else DEFAULT_HOST,
        "no-of-setups": 1,
        "write_directly": False,
        "timeout": 600000  # 10 minutes
    }

    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v.lower() == "true" if v.lower() in ["true", "false"] else v

    paths = PATHS[config["mode"]]

    if not "out" in config:
        config["out"] = paths["path-to-output-dir"]
    if not "csv-out" in config:
        config["csv-out"] = paths["path-to-csv-output-dir"]

    print("consumer config:", config)

    context = zmq.Context()
    socket = context.socket(zmq.PULL)

    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    socket.RCVTIMEO = config["timeout"]
    leave = False
    write_normal_output_files = False
    setup_id_to_nc = {}
    setup_id_to_nc_vars = {}
    setup_id_to_np_arrays = {}

    def init_netcdfs(no_days, no_rows, no_cols, write_directly=False):
        path = config["out"]
        if not os.path.exists(path):
            os.makedirs(path)
        nc_file_path = path + "test.nc"
        if os.path.exists(nc_file_path):
            rootgrp = Dataset(nc_file_path, "a", format="NETCDF4")
            sm1 = rootgrp.variables["sm_sum_0-30"]
            sm2 = rootgrp.variables["sm_sum_30-200"]
            nfc = rootgrp.variables["pnfc_avg_0-30"]
        else:
            rootgrp = Dataset(nc_file_path, "w", format="NETCDF4")
            rootgrp.history = "Created " + time.ctime()
            rootgrp.createDimension("time", None)  # appendable
            rootgrp.createDimension("row", no_rows)
            rootgrp.createDimension("col", no_cols)
            sm1 = rootgrp.createVariable("sm_sum_0-30", "f8", ("time", "row", "col"))
            sm1.description = "soil moisture"
            sm1.units = "fraction"
            sm1.missing_value = -9999
            sm2 = rootgrp.createVariable("sm_sum_30-200", "f8", ("time", "row", "col"))
            sm2.description = "soil moisture"
            sm2.units = "fraction"
            sm2.missing_value = -9999
            nfc = rootgrp.createVariable("pnfc_avg_0-30", "f8", ("time", "row", "col"))
            nfc.description = "percent net field capacity"
            nfc.units = "%"
            nfc.missing_value = -9999

        sm1a = None if write_directly else np.full((no_days, no_rows, no_cols), -9999, dtype="f8")
        sm2a = None if write_directly else np.full((no_days, no_rows, no_cols), -9999, dtype="f8")
        nfca = None if write_directly else np.full((no_days, no_rows, no_cols), -9999, dtype="f8")

        return {"nc": rootgrp, "nc_vars": [sm1, sm2, nfc], "np_arrays": [sm1a, sm2a, nfca]}

    def process_message(msg):
        if len(msg["errors"]) > 0:
            print("There were errors in message:", msg, "\nSkipping message!")
            return

        if not hasattr(process_message, "wnof_count"):
            process_message.wnof_count = 0
            process_message.setup_count = 0

        leave = False

        if not write_normal_output_files:
            custom_id = msg["customId"]
            setup_id = custom_id["setup_id"]

            row = custom_id["srow"]
            col = custom_id["scol"]

            if setup_id not in setup_id_to_nc:
                init = init_netcdfs(custom_id["no_days"], custom_id["no_rows"], custom_id["no_cols"],
                                    config["write_directly"])
                setup_id_to_nc[setup_id] = init["nc"]
                setup_id_to_nc_vars[setup_id] = init["nc_vars"]
                setup_id_to_np_arrays[setup_id] = init["np_arrays"]
            np_arrays = setup_id_to_np_arrays[setup_id]


            debug_msg = f'received work result {process_message.received_env_count} customId: {msg.get("customId", "")}'
            print(debug_msg)

            process_message.received_env_count = process_message.received_env_count + 1
            write_output_to_netcdfs(row, col, msg.get("data", []), np_arrays)

            #if process_message.received_env_count > 100:
            #    leave = True

        elif write_normal_output_files:

            if msg.get("type", "") in ["jobs-per-cell", "no-data", "setup_data"]:
                # print "ignoring", result.get("type", "")
                return

            print("received work result ", process_message.received_env_count, " customId: ",
                  str(list(msg.get("customId", "").values())))

            custom_id = msg["customId"]
            setup_id = custom_id["setup_id"]
            row = custom_id["srow"]
            col = custom_id["scol"]

            process_message.wnof_count += 1

            # with open("out/out-" + str(i) + ".csv", 'wb') as _:
            with open("out-normal/out-" + str(process_message.wnof_count) + ".csv", "w", newline='') as _:
                writer = csv.writer(_, delimiter=";")

                for data_ in msg.get("data", []):
                    results = data_.get("results", [])
                    orig_spec = data_.get("origSpec", "")
                    output_ids = data_.get("outputIds", [])

                    if len(results) > 0:
                        writer.writerow([orig_spec.replace("\"", "")])
                        for row in monica_io3.write_output_header_rows(output_ids,
                                                                       include_header_row=True,
                                                                       include_units_row=True,
                                                                       include_time_agg=False):
                            writer.writerow(row)

                        for row in monica_io3.write_output(output_ids, results):
                            writer.writerow(row)

                    writer.writerow([])

            process_message.received_env_count = process_message.received_env_count + 1

        return leave

    process_message.received_env_count = 1

    while not leave:
        try:
            msg = json.loads(socket.recv_string(encoding="latin-1")) #socket.recv_json(encoding="latin-1")
            leave = process_message(msg)
        except zmq.error.Again as _e:
            print('no response from the server (with "timeout"=%d ms) ' % socket.RCVTIMEO)
            leave = True
        except Exception as e:
            print("Exception:", e)
            leave = True

    for sid, ncvs in setup_id_to_nc_vars.items():
        ncas = setup_id_to_np_arrays[sid]
        for i, ncv in enumerate(ncvs):
            ncv[:, :, :] = ncas[i]

    for _, nc in setup_id_to_nc.items():
        if isinstance(nc, Dataset):
            nc.close()

    print("exiting run_consumer()")


if __name__ == "__main__":
    run_consumer()
