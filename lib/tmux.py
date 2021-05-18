from typing import Optional
import libtmux
import os
import sys

def current_session(serv: libtmux.Server) -> Optional[libtmux.Session]:
    tty = os.ttyname(sys.stdout.fileno())
    for sess in serv.list_sessions():
        for w in sess.list_windows():
            for p in w.list_panes():
                if p.get('pane_tty') == tty:
                    return sess
    return None
