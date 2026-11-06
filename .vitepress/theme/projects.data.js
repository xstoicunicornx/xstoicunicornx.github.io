import fs from 'node:fs'
import path from 'node:path'

const STATUS_ORDER = ['active', 'paused', 'someday', 'done']

export default {
  watch: ['../../data/projects/*.json'],
  load(files) {
    return files
      .map((file) => {
        const project = JSON.parse(fs.readFileSync(file, 'utf-8'))
        return { slug: path.basename(file, '.json'), todos: [], ...project }
      })
      .sort(
        (a, b) =>
          STATUS_ORDER.indexOf(a.status) - STATUS_ORDER.indexOf(b.status) ||
          a.name.localeCompare(b.name)
      )
  }
}
