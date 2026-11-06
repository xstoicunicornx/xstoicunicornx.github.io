import fs from 'node:fs'
import path from 'node:path'

// Projects appear in the order listed in data/order.json.
// Any project missing from that list goes at the end.
export default {
  watch: ['../../data/order.json', '../../data/projects/*.json'],
  load(files) {
    const orderFile = files.find((file) => path.basename(file) === 'order.json')
    const order = JSON.parse(fs.readFileSync(orderFile, 'utf-8'))
    const rank = (slug) => {
      const i = order.indexOf(slug)
      return i === -1 ? order.length : i
    }

    return files
      .filter((file) => file !== orderFile)
      .map((file) => {
        const project = JSON.parse(fs.readFileSync(file, 'utf-8'))
        return { slug: path.basename(file, '.json'), ...project }
      })
      .sort((a, b) => rank(a.slug) - rank(b.slug))
  }
}
