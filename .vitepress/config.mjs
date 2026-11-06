import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'xstoicunicornx',
  description: 'Projects I want to work on and follow up on.',
  cleanUrls: true,
  appearance: false,
  srcExclude: ['README.md'],
  head: [['meta', { name: 'theme-color', content: '#0a1530' }]]
})
