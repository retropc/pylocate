#!/usr/bin/env python
import os, sys, trie, platform, ctypes

MAX_RESULTS = 200
EDITOR = os.environ.get("PYLOCATE_EDITOR")

if platform.system() == "Windows":
  exc = lambda x: ctypes.windll.shell32.ShellExecuteW(0, u'open',  ctypes.c_wchar_p(x), None, None, 1)
  exc_editor = lambda x: ctypes.windll.shell32.ShellExecuteW(0, u'open', unicode(EDITOR), ctypes.c_wchar_p(x), None, 1)
  from indexworker_generic import IndexWorkerGeneric as IndexWorker
else:
  import signal
  SIGNAL_SET = False
  LAUNCHER = os.environ.get("PYLOCATE_LAUNCHER", "xdg-open")

  def pre_exec():
    global SIGNAL_SET
    if not SIGNAL_SET:
      SIGNAL_SET = True
      signal.signal(signal.SIGCHLD, signal.SIG_IGN)

  def exc(x):
    pre_exec()
    os.spawnvp(os.P_NOWAIT, LAUNCHER, [LAUNCHER, x])

  def exc_editor(x):
    pre_exec()
    os.spawnvp(os.P_NOWAIT, EDITOR, [EDITOR, x])

  from indexworker_unix import IndexWorkerUnix as IndexWorker
#from indexworker_generic import IndexWorkerGeneric as IndexWorker

SCRIPTPATH, _ = os.path.split(sys.argv[0])

class PyIndexGUI:
  def __init__(self, i, index_worker):
    self.__base = i.metadata["base"]
    self.__index_worker = index_worker
    self.__data = []

    builder = gtk.Builder()
    builder.add_from_file(os.path.join(SCRIPTPATH, "gui.xml"))

    builder.connect_signals(self)

    self.__window = builder.get_object("window")
    self.__treeview = builder.get_object("results")
    self.__statusbar = builder.get_object("statusbar")
    self.__tag = 0
    self.__last_tag_seen = -1
    self.__timer = False
    self.__setup_treeview()

  def __setup_treeview(self):
    t = self.__treeview
    self.__listmodel = gtk.ListStore(str, str)
    t.set_model(self.__listmodel)

    cell = gtk.CellRendererText()
    col = gtk.TreeViewColumn("Filename")
    col.pack_start(cell, True)
    col.set_attributes(cell, text=0)
    t.append_column(col)

#    col = gtk.TreeViewColumn("Directory")
#    cell = gtk.CellRendererText()
#    col.pack_start(cell)
#    col.set_attributes(cell, text=1)
#    t.append_column(col)

  def show(self):
    self.__window.show()

  def on_window_destroy(self, widget, data=None):
    gtk.main_quit()

  def on_window_key_press_event(self, widget, data=None):
    if data.keyval == gtk.keysyms.Escape:
      self.__window.destroy()

  def get_full_path(self, column):
    return os.path.join(self.__base, self.__data[column[0]])

  def on_results_row_activated(self, widget, column, data=None):
    exc(self.get_full_path(column))
    self.__window.destroy()

  def on_results_button_press_event(self, widget, event):
    if event.type != gtk.gdk.BUTTON_PRESS or event.button not in (2, 3):
      return
    
    tree = self.__treeview.get_path_at_pos(int(event.x), int(event.y))
    if not tree:
      return
      
    p = self.get_full_path(tree[0])
    if event.button == 3:
      p = os.path.dirname(p)
      exc(p)
    if event.button == 2:
      exc_editor(p)
      self.__window.destroy()

  def set_statusbar(self, text):
    cid = self.__statusbar.get_context_id("Status bar")
    self.__statusbar.pop(cid)
    self.__statusbar.push(cid, text)

  def clear(self):
    self.__data = []
    self.__listmodel.clear()
    self.set_statusbar("")
    
  def on_results_cursor_changed(self, widget, data=None):
    row = self.__treeview.get_selection().get_selected_rows()[1][0]
    self.set_statusbar(self.get_full_path(row))

  def __clear_if_no_results(self):
    self.__timer = False
    if self.__tag != self.__last_tag_seen:
      self.__last_tag_seen = self.__tag
      self.clear()

  def on_search_changed(self, widget, data=None):
    t = widget.get_text()

    self.__tag+=1
    if not self.__timer:
      self.__timer = True
      gtk.timeout_add(100, self.__clear_if_no_results)
    self.__index_worker.search(t, self.__tag)

  def add(self, data, tag):
    gtk.gdk.threads_enter()
    try:
      if self.__tag != tag:
        return
      if self.__tag != self.__last_tag_seen:
        self.__last_tag_seen = self.__tag
        self.clear()
      
      for x in data:
        self.__data.append(os.path.join(*x))
        self.__listmodel.append(x[::-1])
      self.__treeview.columns_autosize()
    finally:
      gtk.gdk.threads_leave()
    
def alert(text):
  q = gtk.MessageDialog(None, gtk.DIALOG_DESTROY_WITH_PARENT, gtk.MESSAGE_INFO, gtk.BUTTONS_OK, text.encode("utf-8"))
  q.run()
  q.destroy()

def main(indexfn):
  i = trie.FIndexReadTrie(indexfn)
  try:
    def callback(results, tag):
      window.add(results, tag)

    it = IndexWorker(i, callback, max_results=MAX_RESULTS)
    it.start()
    try:
      window = PyIndexGUI(i, it)
      window.show()
      gtk.gdk.threads_init()
      gtk.main()
    finally:
      it.terminate()
  finally:
    i.close()
    
if __name__ == "__main__":
  if len(sys.argv) < 2:
    alert("usage: %s [index file]" % sys.argv[0])
  elif sys.argv[1] == "--child":
    import indexworker_unix
    indexworker_unix.child(sys.argv[2:])
  else:
    import gtk
    try:
      main(sys.argv[1])
    except KeyboardInterrupt:
      pass
