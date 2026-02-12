import type { Metadata } from '@data-fair/types-catalogs'

const i18n: Metadata['i18n'] = {
  en: {
    description: 'This catalog allows testing the service by simulating a folder and file structure.',
    actionLabels: {
      // createFolderInRoot: 'Create folder in root',
      // createFolder: 'Create folder',
      createResource: 'Create file', // Default : Create resource
      // replaceFolder: 'Replace folder',
      replaceResource: 'Replace file' // Default : Replace resource
    },
    actionButtons: {
      // createFolderInRoot: 'Create publication',
      // createFolder: 'Create publication here',
      createResource: 'Create file here', // Default : Create publication here
      // replaceFolder: 'Replace this folder',
      replaceResource: 'Replace file' // Default : Replace this resource
    },
    stepTitles: {
      // createFolder: 'Destination folder selection',
      // createResource: 'Destination folder selection',
      // replaceFolder: 'Folder to replace selection',
      replaceResource: 'File to replace selection' // Default : Resource to replace selection
    }
  },
  fr: {
    description: 'Ce catalogue permet de tester le service en simulant une arborescence de dossiers et de fichiers.',
    actionLabels: {
      // createFolderInRoot: 'Créer un dossier à la racine',
      // createFolder: 'Créer un dossier',
      createResource: 'Créer un fichier', // Défaut : Créer une ressource
      // replaceFolder: 'Écraser un dossier',
      replaceResource: 'Remplacer un fichier' // Défaut : Écraser une ressource
    },
    actionButtons: {
      // createFolderInRoot: 'Créer la publication',
      // createFolder: 'Créer la publication ici',
      createResource: 'Créer le fichier ici', // Défaut : Créer la publication ici
      // replaceFolder: 'Écraser ce dossier',
      replaceResource: 'Remplacer le fichier' // Défaut : Écraser cette ressource
    },
    stepTitles: {
      // createFolder: 'Sélection du dossier de destination',
      // createResource: 'Sélection du dossier de destination',
      // replaceFolder: 'Sélection du dossier à écraser',
      replaceResource: 'Sélection du fichier à remplacer' // Défaut : Sélection de la ressource à écraser
    }
  }
}

export default i18n
