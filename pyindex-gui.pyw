#!/usr/bin/env python
import gtk, os, sys, trie, platform, ctypes

if platform.system() == "Windows":
  exc = lambda x: ctypes.windll.shell32.ShellExecuteW(0, u'open',  ctypes.c_wchar_p(x), None, None, 1)
else:
  exc = lambda x: os.spawnvp(os.P_NOWAIT, OPENER, ["xdg-open", x])

SCRIPTPATH, _ = os.path.split(sys.argv[0])

class PyIndexGUI:
  def __init__(self, index):
    self.__index = index
    self.__data = []

    builder = gtk.Builder()
    builder.add_from_file(os.path.join(SCRIPTPATH, "gui.xml"))

    builder.connect_signals(self)

    self.__window = builder.get_object("window")
    self.__treeview = builder.get_object("results")

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

  def on_results_row_activated(self, widget, column, data=None):
    file = os.path.join(self.__index.metadata["base"], self.__data[column[0]])
    exc(file)
    self.__window.destroy()

  def on_search_changed(self, widget, data=None):
    t = widget.get_text()
    self.__listmodel.clear()
    self.__data = []

    if t == "":
      return
    for index, x in enumerate(self.__index[t]):
      if index > 25:
        break
      self.__data.append(os.path.join(*x))
      self.__listmodel.append(x[::-1])

def alert(text):
  q = gtk.MessageDialog(None, gtk.DIALOG_DESTROY_WITH_PARENT, gtk.MESSAGE_INFO, gtk.BUTTONS_OK, text.encode("utf-8"))
  q.run()
  q.destroy()

def main(indexfn):
  i = trie.FIndexReadTrie(indexfn)
  try:
    window = PyIndexGUI(i)
    window.show()
    gtk.main()
  finally:
    i.close()

if __name__ == "__main__":
  if len(sys.argv) < 2:
    alert("usage: %s [index file]" % sys.argv[0])
  else:
    main(sys.argv[1])
