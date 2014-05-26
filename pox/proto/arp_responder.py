# Copyright 2011,2012 James McCauley
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
An ARP utility that can learn and proxy ARPs, and can also answer queries
from a list of static entries.

This adds the "arp" object to the console, which you can use to look at
or modify the ARP table.

Add ARP entries on commandline like:
  arp_responder --<IP>=<MAC> --<IP>=<MAC>

Leave MAC unspecified if you want to use the switch MAC.
"""

from pox.core import core
import pox
log = core.getLogger()

from pox.lib.packet.ethernet import ethernet, ETHER_BROADCAST
from pox.lib.packet.arp import arp
from pox.lib.packet.vlan import vlan
from pox.lib.addresses import IPAddr, EthAddr
from pox.lib.util import dpid_to_str, str_to_bool
from pox.lib.recoco import Timer
from pox.lib.revent import EventHalt

import pox.openflow.libopenflow_01 as of

import time


# Timeout for ARP entries
ARP_TIMEOUT = 60 * 4


class Entry (object):
  """
  We use the MAC to answer ARP replies.
  We use the timeout so that if an entry is older than ARP_TIMEOUT, we
   flood the ARP request rather than try to answer it ourselves.
  """
  def __init__ (self, mac, static = None, flood = None):
    self.timeout = time.time() + ARP_TIMEOUT
    self.static = False
    self.flood = True
    if mac is True:
      # Means use switch's MAC, implies static/noflood
      self.mac = True
      self.static = True
      self.flood = False
    else:
      self.mac = EthAddr(mac)

    if static is not None:
      self.static = static
    if flood is not None:
      self.flood = flood

  def __eq__ (self, other):
    if isinstance(other, Entry):
      return (self.static,self.mac)==(other.static,other.mac)
    elif self.mac is True:
      raise AttributeError("Can't comapre MAC on this Entry")
    return self.mac == other
  def __ne__ (self, other):
    return not self.__eq__(other)

  @property
  def is_expired (self):
    if self.static: return False
    return time.time() > self.timeout


class ARPTable (dict):
  def __repr__ (self):
    o = []
    for k,e in self.iteritems():
      t = int(e.timeout - time.time())
      if t < 0:
        t = "X"
      else:
        t = str(t) + "s left"
      if e.static: t = "-"
      mac = e.mac
      if mac is True: mac = "<Switch MAC>"
      o.append((k,"%-17s %-20s %3s" % (k, mac, t)))

    for k,t in _failed_queries.iteritems():
      if k not in self:
        t = int(time.time() - t)
        o.append((k,"%-17s %-20s %3ss ago" % (k, '?', t)))

    o.sort()
    o = [e[1] for e in o]
    o.insert(0,"-- ARP Table -----")
    if len(o) == 1:
      o.append("<< Empty >>")
    return "\n".join(o)

  def __setitem__ (self, key, val):
    key = IPAddr(key)
    if not isinstance(val, Entry):
      val = Entry(val)
    dict.__setitem__(self, key, val)

  def __delitem__ (self, key):
    key = IPAddr(key)
    dict.__delitem__(self, key)

  def set (self, key, value=True, static=True):
    if not isinstance(value, Entry):
      value = Entry(value, static=static)
    self[key] = value


def _handle_expiration ():
  for k,e in _arp_table.items():
    if e.is_expired:
      del _arp_table[k]
  for k,t in _failed_queries.items():
    if time.time() - t > ARP_TIMEOUT:
      del _failed_queries[k]


class ARPResponder (object):
  def __init__ (self):
    # This timer handles expiring stuff
    self._expire_timer = Timer(5, _handle_expiration, recurring=True)

    core.addListeners(self)
    core.listen_to_dependencies(self, ['ARPHelper'])

  def _handle_GoingUpEvent (self, event):
    core.openflow.addListeners(self)
    log.debug("Up...")

  def _learn (self, dpid, a):
    """
    Learn or update port/MAC info.
    a is the arp part of a packet, i.e., a = packet.find('arp').
    """
    if not _learn:
      return

    old_entry = _arp_table.get(a.protosrc)
    if old_entry is None:
      log.info("%s learned %s", dpid_to_str(dpid), a.protosrc)
      _arp_table[a.protosrc] = Entry(a.hwsrc)
    else:
      if old_entry.mac is True:
        # We never replace these special cases.
        # Might want to warn on conflict?
        pass
      elif old_entry.mac != a.hwsrc:
        if old_entry.static:
          log.warn("%s static entry conflict %s: %s->%s",
              dpid_to_str(dpid), a.protosrc, old_entry.mac, a.hwsrc)
        else:
          log.warn("%s RE-learned %s: %s->%s", dpid_to_str(dpid),
              a.protosrc, old_entry.mac, a.hwsrc)
          _arp_table[a.protosrc] = Entry(a.hwsrc)
      else:
        # Update timestamp
        _arp_table[a.protosrc] = Entry(a.hwsrc)

  def _handle_PacketIn (self, event):
    # Note: arp.hwsrc is not necessarily equal to ethernet.src
    # (one such example are arp replies generated by this module itself
    # as ethernet mac is set to switch dpid) so we should be careful
    # to use only arp addresses in the learning code!
    squelch = False

    dpid = event.connection.dpid
    inport = event.port
    packet = event.parsed
    if not packet.parsed:
      log.warning("%s: ignoring unparsed packet", dpid_to_str(dpid))
      return

    a = packet.find('arp')
    if not a: return

    log.debug("%s ARP %s %s => %s", dpid_to_str(dpid),
      {arp.REQUEST:"request",arp.REPLY:"reply"}.get(a.opcode,
      'op:%i' % (a.opcode,)), str(a.protosrc), str(a.protodst))

    if a.prototype == arp.PROTO_TYPE_IP:
      if a.hwtype == arp.HW_TYPE_ETHERNET:
        if a.protosrc != 0:

          self._learn(dpid, a)

          if a.opcode == arp.REQUEST:
            # Maybe we can answer

            if a.protodst in _arp_table:
              # We have an answer...

              r = arp()
              r.hwtype = a.hwtype
              r.prototype = a.prototype
              r.hwlen = a.hwlen
              r.protolen = a.protolen
              r.opcode = arp.REPLY
              r.hwdst = a.hwsrc
              r.protodst = a.protosrc
              r.protosrc = a.protodst
              mac = _arp_table[a.protodst].mac
              if mac is True:
                # Special case -- use ourself
                mac = event.connection.eth_addr
              r.hwsrc = mac
              e = ethernet(type=packet.type, src=event.connection.eth_addr,
                           dst=a.hwsrc)
              e.payload = r
              if packet.type == ethernet.VLAN_TYPE:
                v_rcv = packet.find('vlan')
                e.payload = vlan(eth_type = e.type,
                                 payload = e.payload,
                                 id = v_rcv.id,
                                 pcp = v_rcv.pcp)
                e.type = ethernet.VLAN_TYPE
              log.info("%s answering ARP for %s" % (dpid_to_str(dpid),
                str(r.protosrc)))
              msg = of.ofp_packet_out()
              msg.data = e.pack()
              msg.actions.append(of.ofp_action_output(port =
                                                      of.OFPP_IN_PORT))
              msg.in_port = inport
              event.connection.send(msg)
              return EventHalt if _eat_packets else None
            else:
              # Keep track of failed queries
              squelch = a.protodst in _failed_queries
              _failed_queries[a.protodst] = time.time()

    if self._check_for_flood(dpid, a):
      # Didn't know how to handle this ARP, so just flood it
      msg = "%s flooding ARP %s %s => %s" % (dpid_to_str(dpid),
          {arp.REQUEST:"request",arp.REPLY:"reply"}.get(a.opcode,
          'op:%i' % (a.opcode,)), a.protosrc, a.protodst)

      if squelch:
        log.debug(msg)
      else:
        log.info(msg)

      msg = of.ofp_packet_out()
      msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
      msg.data = event.ofp
      event.connection.send(msg.pack())

    return EventHalt if _eat_packets else None

  def _check_for_flood (self, dpid, a):
    """
    Return True if you want to flood this
    """
    if a.protodst in _arp_table:
      return _arp_table[a.protodst].flood
    return True


_arp_table = ARPTable() # IPAddr -> Entry
_eat_packets = None
_failed_queries = {} # IP -> time : queries we couldn't answer
_learn = None

def launch (timeout=ARP_TIMEOUT, eat_packets=True, no_learn=False, **kw):
  global ARP_TIMEOUT, _install_flow, _eat_packets, _learn
  ARP_TIMEOUT = timeout
  _eat_packets = str_to_bool(eat_packets)
  _learn = not no_learn

  core.Interactive.variables['arp'] = _arp_table
  for k,v in kw.iteritems():
    _arp_table[IPAddr(k)] = Entry(v, static=True)
  core.registerNew(ARPResponder)
