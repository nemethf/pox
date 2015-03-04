# Copyright 2015 Felician Nemeth
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Create two queues for each port at startup time.

Although the ovsdb module contains a small set of usage examples, this
module demonstrates additional functionalities of ovsdb.  The add_que
module adds two queues with different QoS settings to each port
whenever a datapath is connected to POX.  For actual routing decisions
you can use, for example, the l2_qos_learning module.

Test add_que with the following commands.  The packets of the two TCP
connections enqueued in different queues, which results in different
throughput values if they are started at the same time.  Use port 2000
instead of 6000 and the throughput values will be the same.

$ sudo -E mn  --topo=linear,k=3 --mac --controller=remote
$ sudo ./pox.py --verbose log.color ovsdb --connect-back=-1 \
  samples.add_que samples.l2_qos_learning py

mininet> xterm h1 h1 h3 h3
h3:~# iperf -s -u -p 4000 -i 1
h3:~# iperf -s -u -p 6000 -i 1
h1:~# iperf -c 10.0.0.3 -u -p 4000 -b 0.1M
h1:~# iperf -c 10.0.0.3 -u -p 6000 -b 0.1M

POX> core.AddQue.remove()
POX> exit()
"""
from pox.core import core
#import ovsdb

from pox.ovsdb.dsl import *
from pox.ovsdb.dsl import Operation, Statement, MonitorRequest, NO_VALUE
from pox.ovsdb import Row

log = core.getLogger()

class Datapath (object):
  def __init__ (self, dpid):
    self.dpid = dpid
    self.ports = {}             # Uuids of configured ports
    self.conn = None            # Connection to ovsdb

  def update_port (self, conn, uuid):
    log.debug('Datapath.update_port: %s,%s' % (self.dpid, uuid))
    self.conn = conn
    if uuid in self.ports:
      return
    self.ports[uuid] = 1

    self.conn.transact('Open_vSwitch',
      SELECT|['_uuid','qos','name']|FROM|'Port'|WHERE|'_uuid'|EQUAL|["uuid",uuid]
      ).callback(self._update_port_2)

  def _update_port_2 (self, results):
    uuid_qos  = results[0].rows[0]['qos']
    port_name = results[0].rows[0]['name']
    uuid_port = results[0].rows[0]['_uuid']
    log.debug('name:%s|uuid_port:%s|uuid_qos:%s' %
              (port_name, uuid_port, uuid_qos))
    if uuid_qos[0] == 'uuid':
      log.warn('Port has already got QoS settings (%s, %s)' % 
               (port_name, uuid_qos[1]))
      return
    uuid_qos  = ['named-uuid', 'qos']
    uuid_que0 = ['named-uuid', 'que0']
    uuid_que1 = ['named-uuid', 'que1']
    row_qos = Row(queues=["map", [[0, uuid_que0], [1, uuid_que1]]],
                  type="linux-htb",
                  other_config=["map",  [["max-rate", "100000"]]]) # 100 kbps
    row_que0 = {"other_config": ["map", [["priority", "0"],
                                         ["max-rate", "90000"],
                                         ["burst", "1500"]]]}
    row_que1 = {"other_config": ["map", [["priority","1"],
                                         ["min-rate", "10000"],
                                         ["burst", "1500"]]]}

    log.info('%s' % uuid_qos)
    self.conn.transact('Open_vSwitch',
        UPDATE|'Port'|WHERE|"_uuid"|EQUAL|uuid_port|WITH|{"qos": uuid_qos},
        INSERT|row_qos|INTO|'QoS'|WITH|UUID_NAME|uuid_qos[1],
        INSERT|row_que0|INTO|'Queue'|WITH|UUID_NAME|uuid_que0[1],
        INSERT|row_que1|INTO|'Queue'|WITH|UUID_NAME|uuid_que1[1],
        IN|'Open_vSwitch'|MUTATE|'next_cfg'|INCREMENT|1,
        COMMENT|'POX: add two priority queues to port %s' % port_name,
        ).callback(self._update_port_3)

  def _update_port_3 (self, results):
    log.debug('step 3: %s' % results)

  def remove (self):
    """
    Remove everything from Qos and Queue Tables.  

    We could be more clever here, and remove only those entries that
    we added earlier.  But only add_que modifies these tables in the
    example mininet environment.
    """
    log.debug('remove')
    if self.conn is None:
      return
    self.conn.transact('Open_vSwitch',
        DELETE|FROM|'QoS',
        DELETE|FROM|'Queue',
        UPDATE|'Port'|WITH|Row(qos=["set", []]),
        IN|'Open_vSwitch'|MUTATE|'next_cfg'|INCREMENT|1,
        COMMENT|'POX: remove queues',
        ).callback(self._print_result)
    return

  def _print_result (self, results):
    log.info("res:%s" % results)

class SetOvsdbConnection (object):
  def __init__ (self, conn, datapaths):
    self.conn = conn
    self.datapaths = datapaths
    log.debug('step 1')
    self.conn.transact('Open_vSwitch',
                       SELECT|'ports'|AND|'datapath_id'|FROM|'Bridge'
                       ).callback(self._step_2)

  def _step_2 (self, results):
    db = {}
    for row in results[0].rows:
      dpid = int(row.datapath_id, base=16)
      try:
        datapath = self.datapaths[dpid]
      except KeyError:
        continue
      ports = row.ports
      log.debug('port uuids:%s' % ports)
      if ports[0] != 'set':
        continue
      uuids = ports[1]
      for uuid in uuids:
        if uuid[0] != 'uuid':
          continue
        datapath.update_port(self.conn, uuid[1])

class AddQue (object):
  def __init__ (self):
    core.listen_to_dependencies(self)
    self.connections = []
    self.datapaths = {}

  def refresh (self):
    for c in self.connections:
      SetOvsdbConnection(c, self.datapaths)

  def remove (self):
    """Remove QoS settings from every port of every connection."""
    ## _handle_core_GoingDown can't call this method, because ovs
    ## transact needs event processing, and that seems to be shut down
    ## by that time.  Probably.
    ##
    ## As an inconvenient workaround, start pox with the py module,
    ## and clear the ovs database by hand: 
    ## POX> core.AddQue.remove()
    ##
    for d in self.datapaths.itervalues():
      d.remove()

  def _handle_openflow_ConnectionUp (self, event):
    self.datapaths[event.dpid] = Datapath(event.dpid)
    self.refresh()

  def _handle_openflow_ConnectionDown (self, event):
    try:
      del self.datapaths[event.dpid]
    except KeyError:
      pass

  def _handle_OVSDBNexus_ConnectionUp (self, event):
    log.info('ConnectionUp: %s' % event)

    if event.connection not in self.connections:
      self.connections.append(event.connection)
    self.refresh()

  def _handle_core_GoingDownEvent (self, event):
    log.debug('GoingDownEvenet')
    self.remove()               # Too late, won't work.


def launch ():
  core.registerNew(AddQue)
