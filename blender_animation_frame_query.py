import bpy

scn = bpy.context.scene

filename = bpy.data.filepath + '.frame_info'

f = open(filename, 'w')
f.write(str(scn.frame_start))
f.write("\n")
f.write(str(scn.frame_end))
f.write("\n")
f.close()

# make sure specific settings are set correctly in the blend file before
# rendering

# view settings
scn.view_settings.view_transform = 'Filmic Log Encoding Base'
scn.view_settings.look = 'None'

# cycles settings
scn.cycles.transparent_max_bounces = 128

# render settings
scn.render.tile_x = 256
scn.render.tile_y = 256
scn.render.resolution_percentage = 50
scn.render.fps = 24
scn.render.resolution_x = 4096
scn.render.resolution_y = 2160

# save changes
bpy.ops.wm.save_mainfile()
