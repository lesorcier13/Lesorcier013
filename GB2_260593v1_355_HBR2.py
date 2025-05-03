import os
import sys
import time
import shutil
import traceback
import tempfile
import re
import subprocess
import random
import queue
import threading
import concurrent.futures
import PyPDF2
import logging
import warnings
import getpass
from local_files_manager import LocalFilesManager
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QLabel, QPushButton, QFileDialog,
                             QVBoxLayout, QTextEdit, QHBoxLayout, QComboBox, QSpinBox, QCheckBox, QMessageBox,
                             QDialog, QStyle)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QRect
from PyQt5.QtGui import QTextCursor, QFont, QColor, QPalette, QPixmap, QCursor  # Ajout de QCursor

# Supprimer les avertissements et erreurs de PyPDF2
logging.getLogger('PyPDF2').setLevel(logging.ERROR)
warnings.filterwarnings("ignore", category=UserWarning, module="PyPDF2")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="PyPDF2")

try:
    import pikepdf
    PIKEPDF_AVAILABLE = True
except ImportError:
    PIKEPDF_AVAILABLE = False

#Detection du dossier racine pour EXE
def get_program_dir():
    if getattr(sys, 'frozen', False):
        # Si on est congelé (EXE)
        return os.path.dirname(sys.executable)
    else:
        # En mode script normal
        return os.path.dirname(os.path.abspath(__file__))
    
# Ajouter cette classe après les autres imports
class FileLogger:
    """Classe pour gérer la journalisation dans un fichier"""
    
    def __init__(self, filename="autocad_pdf_log.txt"):
        """Initialise le logger avec un chemin de fichier fixe"""
        # Utiliser le dossier local
        self.log_path = os.path.join(get_program_dir(), filename)
        
        # Effacer le fichier existant
        try:
            with open(self.log_path, 'w', encoding='utf-8') as f:
                f.write(f"=== DÉBUT DE SESSION : {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                f.write(f"Version du programme : GB2_260593v1_33\n")
                f.write(f"Utilisateur : {getpass.getuser()}\n")
                f.write(f"Système : {sys.platform}\n\n")
        except Exception as e:
            print(f"Erreur lors de l'initialisation du fichier de log : {str(e)}")
    
    def write(self, message):
        """Écrit un message dans le fichier de log"""
        try:
            with open(self.log_path, 'a', encoding='utf-8') as f:
                timestamp = time.strftime('%H:%M:%S')
                f.write(f"[{timestamp}] {message}\n")
        except Exception:
            pass  # Ignorer les erreurs d'écriture

# Classe pour rediriger stderr (qui contient les messages d'erreur)
class NullWriter:
    def write(self, *args, **kwargs):
        pass
    def flush(self, *args, **kwargs):
        pass


class CursorManager:
    """
    Gestionnaire de curseurs avec suivi d'état pour éviter les problèmes de pile
    """
    def __init__(self, app):
        self.app = app
        self.cursor_stack_count = 0
        self.override_active = False
        # Réinitialisation au démarrage pour nettoyer d'éventuels curseurs résiduels
        self.reset()
        
    
    def set_busy(self):
        """Définit le curseur occupé (sablier) de manière sécurisée"""
        try:
            if not self.override_active:
                self.app.setOverrideCursor(Qt.WaitCursor)
                self.override_active = True
                self.cursor_stack_count = 1  # Réinitialiser le compteur à 1 exactement
            else:
                # Si déjà occupé, ne pas empiler un nouveau curseur
                pass
        except Exception:
            pass  # Ignorer les erreurs liées au curseur
    
    def set_normal(self, force=False):
        """
        Restaure le curseur normal
        Si force=True, vide complètement la pile des curseurs
        """
        try:
            # Si forçage demandé, réinitialiser complètement
            if force:
                self.reset()
                return
                
            # Sinon, restaurer normalement si actif
            if self.override_active:
                self.app.restoreOverrideCursor()
                self.override_active = False
                self.cursor_stack_count = 0
        except Exception:
            # En cas d'erreur, réinitialiser l'état
            self.reset()
    
    def ensure_normal_for_dialog(self, dialog_function, *args, **kwargs):
        """Utilitaire pour exécuter une boîte de dialogue avec curseur normal"""
        was_busy = self.override_active
        self.set_normal()
        
        try:
            result = dialog_function(*args, **kwargs)
        finally:
            # Restaurer l'état précédent si nécessaire
            if was_busy:
                self.set_busy()
                
        return result
    
    def reset(self):
        """Réinitialise complètement l'état des curseurs (en cas de problème)"""
        try:
            # Vider toute la pile potentielle
            while self.app.overrideCursor() is not None:
                self.app.restoreOverrideCursor()
        except Exception:
            pass
            
        # Réinitialiser l'état interne
        self.override_active = False
        self.cursor_stack_count = 0


# Fonction pour extraire le numéro d'un nom de fichier
def extraire_numero(nom_fichier):
    """
    Extrait le numéro du format _Fxx ou _fxx d'un nom de fichier
    Spécialement optimisé pour les noms comme "_F01_plot.pdf"
    """
    # Convertir le nom en minuscules pour une recherche insensible à la casse
    nom_lower = nom_fichier.lower()
    
    # 1. Priorité maximale: Recherche de formats comme "_f01", "_f1" spécifiquement
    match = re.search(r'_f(\d+)_', nom_lower)
    if match:
        num = int(match.group(1))
        return num
    
    # 2. Deuxième priorité: Recherche de formats comme "_f01", "_f1" n'importe où
    match = re.search(r'_f(\d+)', nom_lower)
    if match:
        num = int(match.group(1))
        return num
    
    # 3. Troisième priorité: Recherche de formats comme "_01_", "_1_" spécifiquement
    match = re.search(r'_(\d+)_', nom_lower)
    if match:
        num = int(match.group(1))
        return num
    
    # 4. Quatrième priorité: Recherche de formats comme "_01", "_1" n'importe où
    match = re.search(r'_(\d+)', nom_lower)
    if match:
        num = int(match.group(1))
        return num
    
    # 5. Dernière priorité: Recherche de tout nombre dans le nom
    match = re.search(r'(\d+)', nom_lower)
    if match:
        num = int(match.group(1))
        return num
    
    # Si aucun nombre n'est trouvé, retourner une valeur par défaut
    return 999999  # Grand nombre pour placer à la fin


# Fonction utilitaire pour réessayer les opérations réseau
def operation_avec_retry(fonction, *args, max_tentatives=3, delai_initial=2, **kwargs):
    """Exécute une fonction avec réessai en cas d'erreur réseau"""
    derniere_erreur = None
    delai = delai_initial
    
    for tentative in range(max_tentatives):
        try:
            return fonction(*args, **kwargs)
        except (IOError, OSError, PermissionError) as e:
            derniere_erreur = e
            if tentative < max_tentatives - 1:
                print(f"Erreur réseau: {str(e)}. Nouvelle tentative ({tentative+1}/{max_tentatives}) dans {delai}s...")
                time.sleep(delai)
                delai *= 1.5  # Délai exponentiel
    
    # Si toutes les tentatives ont échoué
    raise derniere_erreur

# Classe pour l'écran de bienvenue
class WelcomeSplash(QDialog):
    def __init__(self, username):
        super().__init__()
        self.setWindowTitle("Bienvenue")
        self.setWindowFlags(Qt.SplashScreen | Qt.FramelessWindowHint)
        
        # Créer un layout
        layout = QVBoxLayout(self)
        
        # Créer un label pour afficher le message de bienvenue
        welcome_label = QLabel(f"Bonjour {username}")
        
        # Configurer la police et la taille
        font = QFont("Arial", 16, QFont.Bold)
        welcome_label.setFont(font)
        welcome_label.setAlignment(Qt.AlignCenter)
        
        # Style du texte - noir sur fond blanc (style simple)
        welcome_label.setStyleSheet("color: black; padding: 20px;")
        
        # Ajouter le label au layout
        layout.addWidget(welcome_label)
        
        # Style simple - fond blanc avec bordure grise fine
        self.setStyleSheet("""
            background-color: white; 
            border: 1px solid #cccccc; 
            border-radius: 5px;
        """)
        self.setFixedSize(400, 150)
        
        # Méthode simple pour centrer la fenêtre
        self.setGeometry(
            QStyle.alignedRect(
                Qt.LeftToRight,
                Qt.AlignCenter,
                self.size(),
                QApplication.desktop().availableGeometry()
            )
        )
        
    def mousePressEvent(self, event):
        # Permet de fermer la fenêtre en cliquant dessus
        self.close()

# Fonction pour formater le nom d'utilisateur
# Fonction pour formater le nom d'utilisateur
def format_username():
    # Récupérer le nom d'utilisateur du système
    username = getpass.getuser()
    
    # Vérifier si le format est prénom.nom
    match = re.match(r'^([a-zA-Z]+)\.([a-zA-Z]+)$', username)
    if match:
        # Formater en Prénom NOM
        prenom = match.group(1).capitalize()
        nom = match.group(2).upper()
        return f"{prenom} {nom}"
    else:
        # Conserver tel quel si le format est différent
        return username

# Cache local pour fichiers réseau - Optimisé avec blocage minimal
class CacheLocalReseau:
    """Gère un cache local pour les fichiers réseau"""
    def __init__(self, dossier_cache=None):
        self.dossier_cache = dossier_cache or os.path.join(tempfile.gettempdir(), "autocad_pdf_cache")
        if not os.path.exists(self.dossier_cache):
            os.makedirs(self.dossier_cache)
        self.fichiers_caches = {}
        self._lock = threading.RLock()  # Utiliser RLock pour permettre des appels imbriqués
    
    def get_fichier_local(self, chemin_reseau):
        """Récupère une copie locale du fichier réseau"""
        # Vérifier d'abord si déjà en cache (sans lock pour optimiser)
        if chemin_reseau in self.fichiers_caches:
            return self.fichiers_caches[chemin_reseau]
            
        # Générer un nom de fichier local unique basé sur le chemin réseau
        nom_fichier = os.path.basename(chemin_reseau)
        chemin_local = os.path.join(self.dossier_cache, f"{int(time.time())}_{nom_fichier}")
        
        # Copier le fichier réseau en local avec retry
        def copier():
            shutil.copy2(chemin_reseau, chemin_local)
            return chemin_local
        
        try:
            chemin_local = operation_avec_retry(copier, max_tentatives=3)
            with self._lock:
                self.fichiers_caches[chemin_reseau] = chemin_local
            return chemin_local
        except Exception as e:
            print(f"Impossible de mettre en cache {chemin_reseau}: {str(e)}")
            return chemin_reseau  # En cas d'échec, retourner le chemin original
    
    def nettoyer(self):
        """Nettoie les fichiers temporaires"""
        with self._lock:
            for chemin_local in self.fichiers_caches.values():
                try:
                    if os.path.exists(chemin_local):
                        os.remove(chemin_local)
                except:
                    pass
            self.fichiers_caches.clear()


# Logger amélioré pour opérations réseau
class ResauLogger:
    """Gestionnaire de log pour opérations réseau"""
    def __init__(self, log_function):
        self.log = log_function
    
    def operation(self, description, fonction, *args, **kwargs):
        """Exécute et log une opération réseau avec timing"""
        self.log(f"[RÉSEAU] Début: {description}...")
        debut = time.time()
        try:
            resultat = fonction(*args, **kwargs)
            duree = time.time() - debut
            self.log(f"[RÉSEAU] Terminé: {description} en {duree:.2f}s")
            return resultat
        except Exception as e:
            duree = time.time() - debut
            self.log(f"[RÉSEAU] ÉCHEC: {description} après {duree:.2f}s - {str(e)}")
            raise


# Fonction pour ouvrir le PDF avec le lecteur par défaut
def open_pdf(pdf_path):
    try:
        if sys.platform == 'win32':
            os.startfile(pdf_path)
        elif sys.platform == 'darwin':  # macOS
            subprocess.call(['open', pdf_path])
        else:  # linux
            subprocess.call(['xdg-open', pdf_path])
        return True
    except Exception as e:
        print(f"Erreur lors de l'ouverture du PDF: {str(e)}")
        return False


# Optimisation: Créer un thread pooler pour réduire le coût de création des threads
class ThreadPoolManager:
    _instance = None
    
    @classmethod
    def get_instance(cls, max_workers=None):
        if cls._instance is None:
            cls._instance = cls(max_workers)
        return cls._instance
    
    def __init__(self, max_workers=None):
        if max_workers is None:
            # Utiliser le nombre de processeurs + 4 pour optimiser les opérations I/O
            import multiprocessing
            max_workers = min(32, multiprocessing.cpu_count() * 2 + 4)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    
    def submit(self, fn, *args, **kwargs):
        return self.executor.submit(fn, *args, **kwargs)
    
    def shutdown(self):
        self.executor.shutdown(wait=False)


# Classe de travail optimisée avec futures
class ProcessWorker(threading.Thread):
    def __init__(self, accore_path, dwg_file, output_dir=None, timeout=300, result_queue=None):
        super().__init__()
        self.accore_path = accore_path
        self.dwg_file = dwg_file
        self.output_dir = output_dir
        self.timeout = timeout
        self.result = {"success": False, "message": "", "pdf_path": ""}
        self.process = None
        self.stopped = False
        self.script_path = None
        self.daemon = True
        self.result_queue = result_queue
        # Augmenter le nombre de tentatives pour les opérations réseau
        self.max_retry = 3
        
    def stop(self):
        self.stopped = True
        if self.process and self.process.poll() is None:
            try:
                self.process.terminate()
                time.sleep(0.5)
                if self.process.poll() is None:
                    self.process.kill()
            except: pass

    def run(self):
        try:
            # Échelonner les démarrages pour éviter les pics de charge réseau
            # Mais avec moins de randomisation pour réduire les délais inutiles
            # if random.random() > 0.7:  # Réduit la probabilité de délai
            #    time.sleep(random.uniform(0.1, 0.5))  # Délai plus court
        
            name = os.path.splitext(os.path.basename(self.dwg_file))[0]
            pdf_file = os.path.join(self.output_dir or os.path.dirname(self.dwg_file), f"{name}_plot.pdf")
            self.result["pdf_path"] = pdf_file
    
            # Vérifier si un autre processus a déjà généré ce PDF
            if os.path.exists(pdf_file) and os.path.getsize(pdf_file) > 0:
                self.result["success"] = True
                self.result["message"] = "PDF existant réutilisé"
                # Mettre à jour result_queue si disponible
                if self.result_queue:
                    self.result_queue.put((self.dwg_file, self.result))
                return
        
            if os.path.exists(pdf_file):
                try: 
                    # Utiliser retry pour supprimer le fichier existant
                    def supprimer():
                        os.remove(pdf_file)
                    operation_avec_retry(supprimer, max_tentatives=self.max_retry)
                except Exception as e: 
                    self.result["message"] = f"Impossible de supprimer le fichier: {pdf_file}: {str(e)}"
                    # Mettre à jour result_queue si disponible
                    if self.result_queue:
                        self.result_queue.put((self.dwg_file, self.result))
                    return

            # Créer le script dans un emplacement local pour éviter problèmes réseau
            fd, self.script_path = tempfile.mkstemp(suffix=".scr")
            os.close(fd)
            with open(self.script_path, 'w', encoding='utf-8') as f:
                f.write(self._create_script_content(pdf_file))

            # Vérifier si le DWG est sur réseau
            is_network_path = self.dwg_file.startswith('\\\\') or ':' in self.dwg_file[:2]
            local_dwg = self.dwg_file
    
            # Adapter le timeout en fonction de la charge et du réseau
            adaptive_timeout = self.timeout
            if is_network_path:
                adaptive_timeout *= 1.5  # Augmenter le timeout pour chemins réseau
            if threading.active_count() > 20:  # Plus strict pour détecter la surcharge
                adaptive_timeout *= 1.2  # Augmentation supplémentaire si beaucoup de workers
    
            # IMPORTANT: Utilisez la commande comme dans la version 30
            # Ne pas ajouter l'option /isolate, elle cause des problèmes
            cmd = [self.accore_path, "/i", local_dwg, "/s", self.script_path]
    
            startupinfo = None
            if hasattr(subprocess, 'STARTUPINFO'):
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = 0

            # Priorité basse pour les processus AutoCAD
            creationflags = 0
            if sys.platform == 'win32':
                creationflags = subprocess.BELOW_NORMAL_PRIORITY_CLASS
    
            # Pas de variables d'environnement spéciales qui pourraient perturber AutoCAD
            self.process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                startupinfo=startupinfo,
                creationflags=creationflags
            )
    
            stdout, stderr = self.process.communicate(timeout=adaptive_timeout)
    
            # Vérifier le fichier PDF avec retry pour les chemins réseau
            def verifier_pdf():
                return os.path.exists(pdf_file) and os.path.getsize(pdf_file) > 0
    
            pdf_ok = False
            if self.process.returncode == 0:
                # Attendre un peu plus pour les chemins réseau
                if is_network_path:
                    time.sleep(0.5)
                # Vérifier avec retry
                try:
                    pdf_ok = operation_avec_retry(verifier_pdf, max_tentatives=self.max_retry, delai_initial=1)
                except:
                    pdf_ok = False
    
            if self.process.returncode != 0:
                # Amélioration de la gestion d'erreur pour afficher le message complet
                error_output = stderr.decode('utf-8', errors='replace')
                self.result["message"] = f"Erreur AutoCAD (code {self.process.returncode}): {error_output}"
            elif pdf_ok:
                self.result["success"] = True
                self.result["message"] = "PDF généré avec succès"
            else:
                self.result["message"] = "Le processus s'est terminé mais aucun PDF n'a été généré"

        except subprocess.TimeoutExpired:
            self.result["message"] = f"Délai d'attente dépassé (timeout={adaptive_timeout}s)"
            if self.process: self.process.kill()
        except Exception as e:
            self.result["message"] = f"Exception: {str(e)}"
            # Ajouter un traceback complet au fichier de log pour le diagnostic
            error_trace = traceback.format_exc()
            print(f"[ERROR TRACE] {error_trace}")  # Le log principal capturera aussi cette sortie
            traceback.print_exc()  # Ajouter un traceback pour aider au diagnostic
        finally:
            if self.script_path and os.path.exists(self.script_path):
                try: os.unlink(self.script_path)
                except: pass
    
            # Mettre à jour result_queue si disponible
            if self.result_queue:
                self.result_queue.put((self.dwg_file, self.result))
    def _create_script_content(self, pdf_path):
        return f'''_PLOT
O
Objet
DWG To PDF.pc3
ISO expand A3 (420.00 x 297.00 MM)
M
A
N
E
P
C
O
monochrome.ctb
N
F
"{pdf_path}"
N
O
'''


# Classe worker optimisée utilisant un pool de threads
class AcCoreConsoleWorker(QThread):
    progress = pyqtSignal(str)
    finished = pyqtSignal(bool, str, list, list)  # Ajout des fichiers DWG originaux

    def __init__(self, accore_path, dwg_files, parallel=5, output_dir=None):
        super().__init__()
        self.accore_path = accore_path
        self.dwg_files = dwg_files
        # Limiter le nombre entre 1 et 30
        self.parallel = max(1, min(parallel, 30))
        self.output_directory = output_dir
        self.is_running = True
        self.active_workers = []
        self.successful_pdfs = []  # Liste pour stocker les chemins des PDF générés avec succès
        self.failed_dwgs = []      # Liste pour stocker les fichiers DWG qui ont échoué
        self.result_queue = queue.Queue()
    
    def stop(self):
        self.is_running = False
        for worker in self.active_workers:
            if worker.is_alive():
                worker.stop()

    def run(self):
        # Initialiser les compteurs et les listes de résultats
        self.successful_pdfs = []
        self.failed_dwgs = []
        total_files = len(self.dwg_files)
    
        self.progress.emit(f"Démarrage du traitement de {total_files} fichiers avec {self.parallel} traitements simultanés...")
    
        # Créer une file d'attente pour les fichiers à traiter
        files_queue = list(self.dwg_files)
    
        # Dictionnaire pour suivre les processus actifs
        active_processes = {}
    
        # Compteurs pour les statistiques
        processed = 0
        success_count = 0
        error_count = 0
    
        # Boucle principale de traitement
        while (files_queue or active_processes) and self.is_running:
            # 1. Compléter jusqu'au nombre maximum de processus parallèles
            spaces_available = self.parallel - len(active_processes)
        
            if spaces_available > 0 and files_queue:
                # Remplir tous les emplacements disponibles
                for _ in range(min(spaces_available, len(files_queue))):
                    if not files_queue or not self.is_running:
                        break
                    
                    dwg_file = files_queue.pop(0)
                    self.progress.emit(f"Préparation de {os.path.basename(dwg_file)}...")
                
                    # Traiter les événements UI pour éviter le blocage
                    QApplication.processEvents()
                
                    worker = ProcessWorker(
                        self.accore_path, 
                        dwg_file, 
                        self.output_directory,
                        timeout=300
                    )
                    worker.start()
                
                    active_processes[dwg_file] = worker
                
                    # Petit délai entre chaque lancement pour éviter les surcharges
                    time.sleep(0.1)
        
            # 2. Vérifier les processus terminés
            to_remove = []
            for dwg_file, worker in list(active_processes.items()):
                if not worker.is_alive():
                    # Le processus est terminé, récupérer le résultat
                    result = worker.result
                    processed += 1
                
                    if result["success"]:
                        self.progress.emit(f"✅ PDF ajouté: {result['pdf_path']}")
                        success_count += 1
                        self.successful_pdfs.append(result['pdf_path'])
                    else:
                        self.progress.emit(f"❌ {os.path.basename(dwg_file)}: {result['message']}")
                        error_count += 1
                        self.failed_dwgs.append(dwg_file)
                
                    # Marquer ce processus pour suppression
                    to_remove.append(dwg_file)
                
                    # Mise à jour du statut
                    self.progress.emit(f"Progrès: {processed}/{total_files} ({success_count} réussis, {error_count} échecs)")
        
            # Supprimer les processus terminés
            for dwg_file in to_remove:
                if dwg_file in active_processes:
                    del active_processes[dwg_file]
        
            # Afficher les statistiques
            if to_remove:
                self.progress.emit(f"[DEBUG] {len(to_remove)} terminés - Actifs restants: {len(active_processes)}/{self.parallel}, Files: {len(files_queue)}")
        
            # Petit délai pour permettre à l'interface de rester réactive
            time.sleep(0.2)
            QApplication.processEvents()
    
        # Si l'exécution est arrêtée, arrêter tous les processus actifs
        if not self.is_running:
            for worker in active_processes.values():
                worker.stop()
    
        # Attendre que tous les processus actifs se terminent
        remaining_timeout = 10  # secondes
        while active_processes and remaining_timeout > 0:
            time.sleep(0.5)
            remaining_timeout -= 0.5
        
            # Vérifier les processus terminés
            to_remove = [dwg_file for dwg_file, worker in active_processes.items() if not worker.is_alive()]
        
            # Supprimer les processus terminés
            for dwg_file in to_remove:
                worker = active_processes[dwg_file]
                result = worker.result
                processed += 1
            
                if result["success"]:
                    self.progress.emit(f"✅ PDF ajouté: {result['pdf_path']}")
                    success_count += 1
                    self.successful_pdfs.append(result['pdf_path'])
                else:
                    self.progress.emit(f"❌ {os.path.basename(dwg_file)}: {result['message']}")
                    error_count += 1
                    self.failed_dwgs.append(dwg_file)
            
                del active_processes[dwg_file]
    
        # Forcer l'arrêt des processus restants
        for worker in active_processes.values():
            worker.stop()
    
        self.finished.emit(error_count == 0, f"Terminé. Réussis: {success_count}, Échecs: {error_count}", self.successful_pdfs, self.failed_dwgs)

# CLASSE PRINCIPALE
class AutoCADPDFInterface(QMainWindow):

    def _add_to_cleanup_list(self, path):
        """Ajoute un chemin à la liste de nettoyage pour exécution future"""
        cleanup_file = os.path.join(tempfile.gettempdir(), "autocad_pdf_cleanup_list.txt")
        try:
            with open(cleanup_file, 'a') as f:
                f.write(f"{path}\n")
        except Exception:
            pass  # Ignorer les erreurs d'écriture du fichier de nettoyage
    
    def set_busy_cursor(self, busy=True):
        """Change le curseur pour indiquer un état occupé ou normal"""
        if busy:
            self.cursor_manager.set_busy()
        else:
            self.cursor_manager.set_normal()

    def ensure_normal_cursor_for_dialog(self, dialog_function, *args, **kwargs):
        """Utilitaire pour exécuter une boîte de dialogue avec curseur normal"""
        return self.cursor_manager.ensure_normal_for_dialog(dialog_function, *args, **kwargs)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("AutoCAD PDF Processor - A3_expand (Optimisé)")
        self.setGeometry(100, 100, 800, 600)

        # Initialiser le logger fichier
        self.file_logger = FileLogger()
    
        # Initialiser le gestionnaire de curseurs
        self.cursor_manager = CursorManager(QApplication.instance())
    
        # Initialiser les variables d'état AVANT de créer l'interface
        self.selected_dwg_files = []
        self.output_directory = None
        self.current_worker = None
        self.parallel_count = 5
        self.final_pdf_path = None
        self.use_pikepdf = PIKEPDF_AVAILABLE
        self.global_start_time = None
        self.using_local_copies = False
        self.local_files_manager = LocalFilesManager(self, self.log)
    
        # Créer d'abord tous les widgets de l'interface
        self.setup_ui()
    
        # Ensuite initialiser les composants qui utilisent l'interface
        self.reseau_logger = ResauLogger(self.log)
        self.cache_local = CacheLocalReseau()
        self.is_network_aware = True
    
        # Détecter les versions d'AutoCAD et remplir l'interface
        self.setup_autocad_versions()

        # AJOUT: Vérifier et nettoyer les dossiers temporaires des exécutions précédentes
        self.check_previous_cleanup_list() 
    
    def setup_ui(self):
        """Crée tous les widgets d'interface"""
        # Widget central et layout principal
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # Bouton d'aide en haut à droite
        top_layout = QHBoxLayout()
        top_layout.addStretch()
        self.help_btn = QPushButton("?")
        self.help_btn.setFixedSize(20, 20)
        self.help_btn.clicked.connect(self.afficher_message_aide)
        top_layout.addWidget(self.help_btn)
        layout.addLayout(top_layout)
        
        # Label pour AutoCAD (sera mis à jour plus tard)
        self.acad_label = QLabel("Choisir AutoCAD:")
        layout.addWidget(self.acad_label)
        
        # Combo box pour AutoCAD (sera rempli plus tard)
        self.acad_combo = QComboBox()
        layout.addWidget(self.acad_combo)
        
        # Boutons de sélection de fichiers DWG dans un layout horizontal
        dwg_files_layout = QHBoxLayout()
        
        # Bouton pour sélectionner les fichiers DWG
        select_btn = QPushButton("Sélectionner des fichiers DWG")
        select_btn.clicked.connect(self.select_dwg_files)
        dwg_files_layout.addWidget(select_btn)
        
        # Bouton pour effacer la liste des fichiers DWG
        clear_btn = QPushButton("Effacer la liste des fichiers DWG")
        clear_btn.clicked.connect(self.clear_dwg_files)
        dwg_files_layout.addWidget(clear_btn)
        
        layout.addLayout(dwg_files_layout)
        
        # Bouton pour le dossier de sortie
        output_btn = QPushButton("Dossier de sortie (facultatif)")
        output_btn.clicked.connect(self.select_output_directory)
        layout.addWidget(output_btn)
        
        # Zones de texte
        self.files_display = QTextEdit()
        self.files_display.setReadOnly(True)
        layout.addWidget(self.files_display)
        
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        layout.addWidget(self.log_display)
        
        # Options avancées
        options_layout = QHBoxLayout()
        
        self.parallel_spinbox = QSpinBox()
        self.parallel_spinbox.setMinimum(3)
        self.parallel_spinbox.setMaximum(30)
        self.parallel_spinbox.setValue(20)
        options_layout.addWidget(QLabel("Fichiers en parallèle:"))
        options_layout.addWidget(self.parallel_spinbox)
        
        fusion_label = "Fusion rapide (recommandé)" if PIKEPDF_AVAILABLE else "Fusion par lots (grands volumes)"
        self.pikepdf_checkbox = QCheckBox(fusion_label)
        self.pikepdf_checkbox.setChecked(self.use_pikepdf)
        self.pikepdf_checkbox.toggled.connect(self.toggle_pikepdf)
        options_layout.addWidget(self.pikepdf_checkbox)
        
        self.cache_checkbox = QCheckBox("Utiliser cache local (optimisation réseau)")
        self.cache_checkbox.setChecked(True)
        options_layout.addWidget(self.cache_checkbox)
        # Case à cocher pour le traitement local des fichiers DWG
        self.local_processing_checkbox = QCheckBox("Copier DWG en local (réseaux lents)")
        self.local_processing_checkbox.setChecked(False)
        self.local_processing_checkbox.setToolTip("Copie temporairement les fichiers DWG près de l'exécutable pour un traitement plus rapide")
        options_layout.addWidget(self.local_processing_checkbox)
        
        layout.addLayout(options_layout)
        
        # Boutons d'action
        button_layout = QHBoxLayout()
        
        run_btn = QPushButton("Exécuter")
        run_btn.clicked.connect(self.run_processing)
        button_layout.addWidget(run_btn)
        
        self.open_pdf_btn = QPushButton("Ouvrir le PDF fusionné")
        self.open_pdf_btn.clicked.connect(self.open_last_pdf)
        self.open_pdf_btn.setEnabled(False)
        button_layout.addWidget(self.open_pdf_btn)
        
        layout.addLayout(button_layout)
    
    def cleanup_other_temp_folders(self):
        """
        Recherche et nettoie les dossiers temporaires,
        optimisé pour les utilisateurs sans droits administrateur
        """
        try:
            # Obtenir le répertoire temporaire de l'utilisateur actuel
            # (accessible sans droits admin)
            user_temp = tempfile.gettempdir()
        
            # Patterns de noms de dossiers créés par notre application
            patterns = ["autocad_pdf_cache", "dwg_temp_", "local_dwg_"]
        
            # Parcourir le dossier temporaire
            self.log("[NETTOYAGE] Recherche de dossiers temporaires...")
        
            count_removed = 0
            problematic_folders = []
        
            for item in os.listdir(user_temp):
                full_path = os.path.join(user_temp, item)
            
                # Vérifier si c'est un dossier et s'il correspond à nos patterns
                if os.path.isdir(full_path) and any(pattern in item for pattern in patterns):
                    self.log(f"[NETTOYAGE] Dossier temporaire trouvé: {full_path}")
                
                    # Vérifier s'il est vide (les dossiers vides sont plus faciles à supprimer)
                    try:
                        is_empty = len(os.listdir(full_path)) == 0
                        if is_empty:
                            try:
                                os.rmdir(full_path)
                                if not os.path.exists(full_path):
                                    count_removed += 1
                                    self.log(f"[NETTOYAGE] ✅ Dossier vide supprimé: {full_path}")
                                    continue
                            except Exception:
                                pass
                    except Exception:
                        pass
                
                    # Si le dossier n'est pas vide ou n'a pas pu être supprimé simplement
                    try:
                        # Tenter la suppression complète
                        self.force_directory_deletion(full_path)
                        if not os.path.exists(full_path):
                            count_removed += 1
                            self.log(f"[NETTOYAGE] ✅ Dossier temporaire supprimé: {full_path}")
                        else:
                            self.log(f"[NETTOYAGE] ⚠️ Échec de suppression: {full_path}")
                            problematic_folders.append(full_path)
                    except Exception as e:
                        self.log(f"[NETTOYAGE] ❌ Erreur: {str(e)}")
                        problematic_folders.append(full_path)
        
            if count_removed > 0:
                self.log(f"[NETTOYAGE] Total: {count_removed} dossiers temporaires supprimés.")
        
            # Essayer de nettoyer les dossiers problématiques au démarrage prochain
            if problematic_folders:
                self.log("[NETTOYAGE] Certains dossiers seront nettoyés au prochain démarrage.")
                for folder in problematic_folders:
                    self._add_to_cleanup_list(folder)
                
        except Exception as e:
            self.log(f"[NETTOYAGE] Erreur lors du nettoyage des dossiers temporaires: {str(e)}")
    
    def clear_dwg_files(self):
        """Effacer la liste des fichiers DWG et réinitialiser les listes associées"""
        # Effacer la liste des fichiers DWG sélectionnés
        self.selected_dwg_files = []
    
        # CORRECTION: Réinitialiser toutes les listes de PDFs et caches associés
        if hasattr(self, 'verified_pdfs'):
            self.verified_pdfs = []
        
        if hasattr(self, 'precached_pdfs'):
            self.precached_pdfs = []
        
        # Réinitialiser l'état de la fusion
        if hasattr(self, 'fusion_preparation_started'):
            self.fusion_preparation_started = False
    
        # Nettoyer le cache si utilisé
        if hasattr(self, 'cache_local'):
            self.cache_local.nettoyer()
    
        # Désactiver le bouton d'ouverture du PDF
        self.open_pdf_btn.setEnabled(False)
        self.final_pdf_path = None
    
        # Effacer l'affichage des fichiers
        self.files_display.clear()
        self.log("Liste des fichiers DWG effacée et listes associées réinitialisées")
    
    def setup_autocad_versions(self):
        """Détecte et configure les versions d'AutoCAD disponibles"""
        self.accore_paths = self._detect_accoreconsole()
        
        # Remplir le combo box
        self.acad_combo.clear()
        for version, path in self.accore_paths:
            self.acad_combo.addItem(version, path)
        
        # Configurer le comportement selon les versions disponibles
        if len(self.accore_paths) == 1 and not any(year in self.accore_paths[0][0] for year in ["2020", "2021"]):
            # Version imposée (2022+)
            self.acad_combo.setEnabled(False)
            self.acad_label.setText(f"Version AutoCAD imposée: {self.accore_paths[0][0]}")
            self.log(f"Version imposée: {self.accore_paths[0][0]}")
        else:
            # Versions au choix (2020/2021)
            self.acad_label.setText("Choisir AutoCAD:")
            if self.accore_paths:
                versions_str = ", ".join(v[0] for v in self.accore_paths)
                self.log(f"Versions disponibles au choix: {versions_str}")
        
        if PIKEPDF_AVAILABLE:
            self.log("✅ Mode fusion rapide (pikepdf) disponible")
    
    def _detect_accoreconsole(self):
        """Détecte les versions d'AutoCAD et applique la logique de sélection spéciale pour 2020/2021"""
        all_paths = []  # Initialisation de all_paths comme une liste vide
        special_versions = []  # Pour stocker 2020 et 2021
        imposed_version = None  # Pour stocker la version imposée (2022+)
        
        # Vérifier toutes les versions de 2025 à 2020
        for year in range(2025, 2019, -1):
            path = f"C:\\Program Files\\Autodesk\\AutoCAD {year}\\accoreconsole.exe"
            if os.path.exists(path):
                if year in [2020, 2021]:
                    # Ajouter 2020 et 2021 à la liste spéciale
                    special_versions.append((f"AutoCAD {year}", path))
                else:
                    # Pour 2022+, imposer la première version trouvée
                    if imposed_version is None:
                        imposed_version = (f"AutoCAD {year}", path)
        
        # Logique de sélection finale
        if imposed_version is not None:
            # Si une version 2022+ est trouvée, l'imposer
            all_paths = [imposed_version]
        elif len(special_versions) > 0:
            # Si des versions spéciales (2020/2021) sont trouvées, proposer le choix
            all_paths = special_versions
        
        return all_paths

    from PyQt5.QtWidgets import QMessageBox, QLabel

    def afficher_message_aide(self):
        # Toujours restaurer le curseur normal avant d'afficher un message
        self.set_busy_cursor(False)
    
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Aide rapide")
        msg_box.setIcon(QMessageBox.Information)
        msg_box.setStandardButtons(QMessageBox.Ok)

        # Créer un QLabel avec ton texte
        label = QLabel("""
    Version optimisée du convertisseur DWG vers PDF.

    1. Sélectionnez des fichiers DWG
    2. Choisissez un dossier de sortie (facultatif)
    3. Ajustez le nombre de processus parallèles selon la puissance de votre machine
    4. Cliquez sur "Exécuter" pour lancer la conversion
    5. Une fois terminé, vous pourrez fusionner les PDFs générés
    6. Allez l'OM
    Cette version utilise des optimisations avancées de threading et de gestion mémoire.
    """)

        # Forcer une largeur maximale sur le label pour éviter un message tout petit
        label.setWordWrap(True)  # Le texte retournera à la ligne naturellement
        label.setMinimumWidth(600)  # Par exemple 600 pixels minimum

        # Remplacer le texte du QMessageBox par ton QLabel
        msg_box.layout().addWidget(label, 0, 1)  # 0 ligne, 1 colonne pour centrer

        msg_box.exec_()



    def toggle_pikepdf(self, checked):
        self.use_pikepdf = checked
        if checked and not PIKEPDF_AVAILABLE:
            self.log("⚠️ Mode fusion rapide non disponible. Utilisation de la fusion par lots.")

    def select_dwg_files(self):
        # Obtenir le chemin où se trouve le programme
        program_dir = get_program_dir()

        # Si des fichiers ont déjà été sélectionnés, utiliser leur dossier comme point de départ
        start_dir = program_dir
        if self.selected_dwg_files:
            start_dir = os.path.dirname(self.selected_dwg_files[0])

        # S'assurer que le curseur est normal avant l'interaction
        self.set_busy_cursor(False)
    
        # Boîte de dialogue de sélection des fichiers
        files, _ = QFileDialog.getOpenFileNames(self, "Sélection DWG", start_dir, "Fichiers AutoCAD (*.dwg)")

        if files:
            # Changer le curseur si beaucoup de fichiers sont sélectionnés
            if len(files) > 20:
                self.set_busy_cursor(True)
            
            # MODIFICATION: Si l'utilisateur sélectionne de nouveaux fichiers, on réinitialise d'abord
            # toutes les listes pour éviter les doublons
            if hasattr(self, 'verified_pdfs'):
                self.verified_pdfs = []
        
            if hasattr(self, 'precached_pdfs'):
                self.precached_pdfs = []
    
            # Réinitialiser l'état de la fusion
            if hasattr(self, 'fusion_preparation_started'):
                self.fusion_preparation_started = False
        
            # Puis on définit la nouvelle liste de fichiers
            self.selected_dwg_files = files
            self.files_display.setPlainText("\n".join(files))
        
            # Restaurer le curseur normal
            if len(files) > 20:
                self.set_busy_cursor(False)
            
            self.log(f"{len(files)} fichiers DWG sélectionnés")

    def select_output_directory(self):
        # Obtenir le chemin où se trouve le programme
        program_dir = get_program_dir()

        # Si un dossier de sortie a déjà été sélectionné, l'utiliser comme point de départ
        # Sinon, si des fichiers DWG ont été sélectionnés, utiliser leur dossier
        start_dir = program_dir
        if self.output_directory:
            start_dir = self.output_directory
        elif self.selected_dwg_files:
            start_dir = os.path.dirname(self.selected_dwg_files[0])
    
        # Restaurer le curseur normal avant l'interaction avec l'utilisateur
        self.set_busy_cursor(False)
    
        folder = QFileDialog.getExistingDirectory(self, "Choisir dossier de sortie", start_dir)
        if folder:
            self.output_directory = folder
            self.log(f"Dossier de sortie sélectionné: {folder}")

    def run_processing(self):
    
        self.verify_cleanup()

        """Traite les fichiers DWG avec AcCoreConsole"""
        if not self.selected_dwg_files:
            self.set_busy_cursor(False)  # Restaurer le curseur normal pour le message d'erreur
            QMessageBox.warning(self, "Attention", "Veuillez sélectionner au moins un fichier DWG.")
            return
    
        path = self.acad_combo.currentData()
        if not path:
            self.set_busy_cursor(False)  # Restaurer le curseur normal pour le message d'erreur
            QMessageBox.warning(self, "Attention", "Aucune version d'AutoCAD détectée.")
            return
    
        # AJOUT: Réinitialiser les listes de PDFs au début d'un nouveau traitement
        # pour éviter les doublons avec un traitement précédent
        if hasattr(self, 'verified_pdfs'):
            self.verified_pdfs = []
    
        if hasattr(self, 'precached_pdfs'):
            self.precached_pdfs = []

        # AJOUT: Réinitialiser l'état de la fusion
        if hasattr(self, 'fusion_preparation_started'):
            self.fusion_preparation_started = False

        # Avertissement si le parallélisme est élevé
        if self.parallel_spinbox.value() > 15:
            # Restaurer le curseur normal pendant l'interaction
            self.set_busy_cursor(False)
        
            reply = QMessageBox.question(self, 'Parallélisme élevé', 
                                      f"Un traitement à {self.parallel_spinbox.value()} fichiers en parallèle peut consommer beaucoup de ressources système.\n\nContinuer quand même?",
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if reply == QMessageBox.No:
                return
        
            # Réactiver le curseur occupé si l'utilisateur choisit de continuer
            self.set_busy_cursor(True)

        # NOUVELLE LIGNE: Définir le curseur occupé
        self.set_busy_cursor(True)

        # Nettoyer d'abord les anciennes copies locales si elles existent
        self.local_files_manager.cleanup()

        # Préparer les fichiers à traiter (originaux par défaut)
        files_to_process = self.selected_dwg_files
        self.using_local_copies = False

        # Si l'option de traitement local est activée
        if self.local_processing_checkbox.isChecked():
            self.log("[LOCAL] Préparation des copies locales...")
    
            # Créer les copies locales
            if self.local_files_manager.create_local_copies(self.selected_dwg_files):
                # Remplacer les chemins réseau par les chemins locaux
                files_to_process = self.local_files_manager.prepare_file_list(self.selected_dwg_files)
                self.using_local_copies = True
                self.log(f"[LOCAL] Utilisation de {len(self.local_files_manager.local_dwg_copies)} copies locales")
            else:
                # Si la copie a échoué ou a été annulée
                self.log("[LOCAL] Utilisation des fichiers originaux")

        # Démarrer le chronomètre global
        self.global_start_time = time.time()
        self.log(f"[CHRONO] Début du traitement global: {time.strftime('%H:%M:%S')}")

        # Désactiver le bouton d'ouverture du PDF pendant le traitement
        self.open_pdf_btn.setEnabled(False)
        self.final_pdf_path = None
    
        # Lancer le worker avec les fichiers à traiter
        self.current_worker = AcCoreConsoleWorker(path, files_to_process, self.parallel_spinbox.value(), self.output_directory)
        self.current_worker.progress.connect(self.log)

        # Si on utilise des copies locales, convertir les chemins d'erreur en chemins originaux
        if self.using_local_copies:
            self.current_worker.finished.connect(
                lambda success, msg, pdfs, failed_dwgs: 
                self.traitement_termine(success, msg, pdfs, 
                                      [self.local_files_manager.get_original_path(d) for d in failed_dwgs])
            )
        else:
            self.current_worker.finished.connect(
                lambda success, msg, pdfs, failed_dwgs: 
                self.traitement_termine(success, msg, pdfs, failed_dwgs)
            )

        self.current_worker.start()

    def est_chemin_reseau(self, chemin):
        """Détermine si un chemin est sur le réseau"""
        if sys.platform == 'win32':
            return chemin.startswith('\\\\') or (len(chemin) > 1 and chemin[0].isalpha() and chemin[1] == ':' and not os.path.splitdrive(chemin)[0].lower() in ['c:', 'd:'])
        else:
            return '//' in chemin or chemin.startswith('smb://') or chemin.startswith('nfs://')

    def precache_network_files(self, pdf_files):
        """Précache les fichiers réseau en arrière-plan pendant le traitement"""
        if not hasattr(self, 'precached_pdfs'):
            self.precached_pdfs = []
        
        for pdf in pdf_files:
            try:
                # Utiliser le cache local existant
                local_path = self.cache_local.get_fichier_local(pdf)
                self.precached_pdfs.append(pdf)
                # Ne pas loguer chaque fichier pour éviter de surcharger l'interface
            except:
                pass  # Ignorer les erreurs, on ne fait que du précaching optionnel

    def verify_cleanup(self):
        """
        Vérifie que le nettoyage a été effectué correctement et force le nettoyage si nécessaire
        """
        if hasattr(self, 'local_files_manager') and hasattr(self.local_files_manager, 'local_temp_dir'):
            if self.local_files_manager.local_temp_dir and os.path.exists(self.local_files_manager.local_temp_dir):
                self.log("[LOCAL] Détection de fichiers temporaires non nettoyés, nettoyage forcé...")
                try:
                    # Forcer un nettoyage complet
                    self.local_files_manager.cleanup()
                
                    # Double vérification
                    if self.local_files_manager.local_temp_dir and os.path.exists(self.local_files_manager.local_temp_dir):
                        # Tentative plus agressive avec notre méthode de suppression forcée
                        try:
                            self.force_directory_deletion(self.local_files_manager.local_temp_dir)
                        except:
                            self.log("[LOCAL] Échec du nettoyage forcé")
                except Exception as e:
                    self.log(f"[LOCAL] Erreur lors du nettoyage forcé: {str(e)}")
    
    def traitement_termine(self, success, msg, successful_pdfs, failed_dwgs):
        self.log(msg)

        # OPTIMISATION 1: Début de préparation de la fusion pendant le traitement
        # Cette variable globale permet de commencer à préparer la fusion progressivement
        if not hasattr(self, 'fusion_preparation_started'):
            self.fusion_preparation_started = True
            self.verified_pdfs = []  # Liste pour collecter les PDFs valides au fur et à mesure
    
        # OPTIMISATION 2: Mise en cache progressive des fichiers réseau pendant le traitement
        # Nous allons précacher les PDF au fur et à mesure qu'ils sont créés
        if self.cache_checkbox.isChecked():
            network_pdfs = [pdf for pdf in successful_pdfs if self.est_chemin_reseau(pdf) and pdf not in getattr(self, 'precached_pdfs', [])]
            if network_pdfs:
                # Utiliser un thread pour ne pas bloquer l'interface
                threading.Thread(target=self.precache_network_files, args=(network_pdfs,), daemon=True).start()

        # OPTIMISATION 3-4: Paralléliser la vérification des fichiers
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            # Fonction pour vérifier un PDF
            def verify_pdf(pdf_path):
                if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                    return pdf_path
                return None
    
            # Vérifier tous les PDFs déjà trouvés
            future_to_pdf = {executor.submit(verify_pdf, pdf): pdf for pdf in successful_pdfs}
            for future in concurrent.futures.as_completed(future_to_pdf):
                verified_pdf = future.result()
                if verified_pdf:
                    if not hasattr(self, 'verified_pdfs'):
                        self.verified_pdfs = []
                    self.verified_pdfs.append(verified_pdf)

        # OPTIMISATION 5: Vérifier l'accès aux fichiers
        def test_file_access(path):
            try:
                with open(path, 'rb') as f:
                    return True
            except:
                return False

        # Vérifier que tous les fichiers sont accessibles avant de continuer
        all_accessible = all(test_file_access(pdf) for pdf in self.verified_pdfs[:5] if self.verified_pdfs) if self.verified_pdfs else False

        if self.verified_pdfs:
            self.log(f"[INFO] {len(self.verified_pdfs)} PDF valides sur {len(self.selected_dwg_files)} attendus")
    
            # MODIFICATION: Stocker les chemins PDF originaux pour la fusion
            original_pdf_paths = list(self.verified_pdfs)
    
            # Lancer la fusion immédiatement, AVANT le nettoyage
            if all_accessible:
                self.fusionner_pdfs_depuis_liste(original_pdf_paths)
            else:
                # Attendre seulement si les fichiers ne sont pas accessibles
                self.log("[INFO] Certains PDF sont encore en cours d'écriture, court délai...")
                time.sleep(0.5)  # Délai réduit
                self.fusionner_pdfs_depuis_liste(original_pdf_paths)
    
            # IMPORTANT: Nettoyer les DWG locaux APRÈS avoir terminé la fusion
            if self.using_local_copies:
                self.log("[LOCAL] Fusion terminée, nettoyage des copies locales...")
                # Utiliser le curseur occupé pour le nettoyage
                self.set_busy_cursor(True)
                self.local_files_manager.cleanup()
                self.using_local_copies = False
        else:
            self.log("❌ Aucun PDF valide pour la fusion.")
    
            # Nettoyer même en cas d'échec
            if self.using_local_copies:
                # Utiliser le curseur occupé pour le nettoyage
                self.set_busy_cursor(True)
                self.local_files_manager.cleanup()
                self.using_local_copies = False
    
        # NOUVELLE LIGNE: Restaurer le curseur normal à la fin du traitement
        # C'est crucial que cette ligne soit la dernière pour garantir que le curseur est normal
        self.set_busy_cursor(False)
    
    # Fonction améliorée pour fusionner les PDF avec gestion correcte du tri
    def fusionner_pdfs_depuis_liste(self, pdfs):
        try:
            # Préparation de la fusion - changer le curseur
            self.set_busy_cursor(True)
        
            # OPTIMISATION 1: Utiliser les PDFs pré-cachés si disponibles
            cached_pdf_dict = {}
            if hasattr(self, 'precached_pdfs') and self.cache_checkbox.isChecked():
                for pdf in self.precached_pdfs:
                    if pdf in pdfs:
                        cached_pdf_dict[pdf] = self.cache_local.get_fichier_local(pdf)

            # OPTIMISATION 2: Déterminer si on travaille principalement sur réseau
            # Le comptage est plus efficace avec les dictionnaires
            network_pdfs = sum(1 for pdf in pdfs if pdf not in cached_pdf_dict and self.est_chemin_reseau(pdf))
            is_mainly_network = network_pdfs > len(pdfs) / 2

            if is_mainly_network:
                self.log(f"[RÉSEAU] Détecté {network_pdfs}/{len(pdfs)} fichiers sur réseau")

            # OPTIMISATION 3: Proposer un nom de fichier intelligent
            # Utiliser le nom du dossier contenant les DWG ou un nom descriptif
            default_name = ""
            if self.output_directory:
                output_dir_name = os.path.basename(os.path.normpath(self.output_directory))
                default_name = os.path.join(self.output_directory, f"{output_dir_name}_fusion.pdf")
            elif self.selected_dwg_files:
                parent_dir = os.path.dirname(self.selected_dwg_files[0])
                dir_name = os.path.basename(os.path.normpath(parent_dir))
                default_name = os.path.join(parent_dir, f"{dir_name}_fusion.pdf")
        
            # Restaurer le curseur normal pendant que l'utilisateur choisit le fichier
            self.set_busy_cursor(False)
            save_path, _ = QFileDialog.getSaveFileName(self, "Nom du PDF final", default_name, "PDF Files (*.pdf)")
            if not save_path:
                self.log("Fusion annulée.")
                # IMPORTANT: Ne PAS nettoyer les fichiers ici - sera fait dans traitement_termine
                return
            
            # Réactiver le curseur occupé après la boîte de dialogue - processus de fusion en cours
            self.log("[FUSION] Préparation de la fusion des PDFs...")
            self.set_busy_cursor(True)

            # Créer le répertoire de destination si nécessaire
            save_dir = os.path.dirname(save_path)
            if save_dir and not os.path.exists(save_dir):
                os.makedirs(save_dir)
        
            # OPTIMISATION 4: Trier les PDFs en parallèle
            # Extraction des numéros et tri en utilisant un ThreadPool
            pdf_with_numbers = []
    
            def extract_number(pdf):
                base_name = os.path.basename(pdf)
                special_match = re.search(r'_[Ff](\d+)_', base_name)
                if special_match:
                    num = 1000000 + int(special_match.group(1))
                else:
                    num = extraire_numero(base_name)
                return (pdf, num)
    
            # Extraction parallèle des numéros
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(pdfs))) as executor:
                pdf_with_numbers = list(executor.map(extract_number, pdfs))
    
            # Tri (opération rapide, pas besoin de paralléliser)
            pdf_with_numbers.sort(key=lambda x: (x[1], os.path.basename(x[0])))
            pdfs_tries = [item[0] for item in pdf_with_numbers]
        
            # OPTIMISATION 5: Mise en cache optimisée pour les fichiers réseau
            # Utiliser les fichiers déjà en cache + mise en cache des nouveaux en parallèle
            if is_mainly_network and self.cache_checkbox.isChecked():
                self.log("[RÉSEAU] Optimisation des accès réseau...")
        
                pdfs_to_merge = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                    # Fonction pour obtenir le chemin local (cache ou original)
                    def get_local_path(pdf):
                        if pdf in cached_pdf_dict:
                            return cached_pdf_dict[pdf]  # Utiliser le cache existant
                        elif self.est_chemin_reseau(pdf):
                            return self.cache_local.get_fichier_local(pdf)  # Nouveau cache
                        else:
                            return pdf  # Fichier local, pas besoin de cache
            
                    # Traiter tous les fichiers en parallèle
                    future_to_pdf = {executor.submit(get_local_path, pdf): pdf for pdf in pdfs_tries}
            
                    # Dictionnaire pour conserver l'ordre
                    local_paths = {}
                    for future in concurrent.futures.as_completed(future_to_pdf):
                        original_pdf = future_to_pdf[future]
                        try:
                            local_path = future.result()
                            local_paths[original_pdf] = local_path
                        except Exception:
                            local_paths[original_pdf] = original_pdf  # En cas d'échec, utiliser l'original
            
                    # Reconstruire la liste dans l'ordre trié
                    pdfs_to_merge = [local_paths.get(pdf, pdf) for pdf in pdfs_tries]
            else:
                pdfs_to_merge = pdfs_tries
        
            # OPTIMISATION 6: Détection plus fine du type de fusion à utiliser
            # Pour les gros volumes (>50 fichiers), préférer la fusion par lots même avec pikepdf
            fusion_start_time = time.time()
            total_files = len(pdfs_to_merge)
    
            if self.use_pikepdf and PIKEPDF_AVAILABLE:
                if total_files > 50:
                    self.log("[FUSION] Volume important détecté, utilisation de la fusion par lots optimisée")
                    success = self.fusionner_avec_pikepdf_par_lots(pdfs_to_merge, save_path)
                else:
                    success = self.fusionner_avec_pikepdf(pdfs_to_merge, save_path)
            else:
                # Fallback à PyPDF2
                success = self.fusionner_avec_pypdf2(pdfs_to_merge, save_path)
            
            fusion_end_time = time.time()
            fusion_duration = fusion_end_time - fusion_start_time
        
            # Ne pas bloquer l'interface pendant le nettoyage
            def async_cleanup():
                if is_mainly_network and self.cache_checkbox.isChecked():
                    self.cache_local.nettoyer()
            
            # Lancer le nettoyage en arrière-plan
            threading.Thread(target=async_cleanup, daemon=True).start()
    
            # Calculer la durée globale si le chronomètre a été démarré
            global_duration = None
            if self.global_start_time is not None:
                global_duration = fusion_end_time - self.global_start_time
        
            # Nettoyer le cache si utilisé
            if is_mainly_network and self.cache_checkbox.isChecked():
                self.cache_local.nettoyer()
                self.log("[CACHE] Cache local nettoyé")
            
            if success and os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                # Afficher les deux durées: fusion seule et traitement global
                self.log(f"✅ Fusion réussie en {fusion_duration:.2f} secondes! -> {save_path}")
        
                if global_duration:
                    minutes = int(global_duration) // 60
                    secondes = int(global_duration) % 60
                    self.log(f"⏱️ Temps total d'exécution: {minutes}min {secondes}s ({global_duration:.2f}s)")
        
                # Stocker le chemin du PDF final
                self.final_pdf_path = save_path
                self.open_pdf_btn.setEnabled(True)
        
                # Restaurer le curseur normal avant d'afficher le message de confirmation
                self.set_busy_cursor(False)
            
                # Message de confirmation avec les deux durées
                message = f"Fusion réussie en {fusion_duration:.2f} secondes!"
                if global_duration:
                    message += f"\nTemps total de traitement: {minutes}min {secondes}s"
                message += f"\n\nFichier créé: {save_path}\n\nVoulez-vous ouvrir le PDF maintenant?"
        
                reply = QMessageBox.question(self, 'Succès', message, QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
        
                if reply == QMessageBox.Yes:
                    open_pdf(save_path)
        
                # Proposer de supprimer les PDF temporaires
                reply = QMessageBox.question(self, 'Nettoyage', 
                                          'Voulez-vous supprimer les fichiers PDF temporaires?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
        
                if reply == QMessageBox.Yes:
                    # Activer le curseur occupé uniquement pour la suppression
                    self.set_busy_cursor(True)
                
                    # IMPORTANT: Ne pas nettoyer les fichiers locaux ici car ils seront nettoyés
                    # après cette fonction dans traitement_termine()
            
                    # Supprimer uniquement les PDFs non locaux
                    if self.using_local_copies and hasattr(self.local_files_manager, 'local_temp_dir'):
                        temp_dir = self.local_files_manager.local_temp_dir
                        pdfs_non_locaux = [pdf for pdf in pdfs_tries if not pdf.startswith(temp_dir)]
                        if pdfs_non_locaux:
                            self.supprimer_pdfs_temporaires(pdfs_non_locaux)
                    else:
                        # Si pas de copies locales, supprimer tous les PDFs
                        self.supprimer_pdfs_temporaires(pdfs_tries)
                
                    # IMPORTANT: Restaurer le curseur normal après la suppression
                    self.set_busy_cursor(False)
            else:
                # Restaurer le curseur normal en cas d'échec
                self.set_busy_cursor(False)
            
                self.log("❌ Échec de la fusion")
            
                # IMPORTANT: Ne pas nettoyer ici, sera fait dans traitement_termine
            
                # Si échec avec pikepdf, essayer PyPDF2
                if self.use_pikepdf and PIKEPDF_AVAILABLE:
                    # Demander à l'utilisateur s'il souhaite essayer la méthode alternative
                    reply = QMessageBox.question(self, 'Échec de fusion', 
                                  'La fusion rapide a échoué. Voulez-vous essayer la méthode alternative (plus lente mais plus compatible)?',
                                  QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
                
                    if reply == QMessageBox.Yes:
                        self.log("Tentative de fusion avec méthode alternative...")
                        # Réactiver le curseur occupé pour la méthode alternative
                        self.set_busy_cursor(True)
                    
                        success = self.fusionner_avec_pypdf2(pdfs_to_merge, save_path)
                
                        if success and os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                            # Restaurer le curseur normal après succès
                            self.set_busy_cursor(False)
                        
                            self.log(f"✅ Fusion réussie avec méthode alternative! -> {save_path}")
                
                            if global_duration:
                                minutes = int(global_duration) // 60
                                secondes = int(global_duration) % 60
                                self.log(f"⏱️ Temps total d'exécution: {minutes}min {secondes}s ({global_duration:.2f}s)")
                
                            self.final_pdf_path = save_path
                            self.open_pdf_btn.setEnabled(True)
                
                            message = f"Fusion réussie avec méthode alternative!"
                            if global_duration:
                                message += f"\nTemps total de traitement: {minutes}min {secondes}s"
                            message += f"\n\nFichier créé: {save_path}\n\nVoulez-vous ouvrir le PDF maintenant?"
                
                            reply = QMessageBox.question(self, 'Succès', message, QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
                
                            if reply == QMessageBox.Yes:
                                open_pdf(save_path)
                
                            # Proposer de supprimer les PDF temporaires après la méthode alternative
                            reply = QMessageBox.question(self, 'Nettoyage', 
                                                      'Voulez-vous supprimer les fichiers PDF temporaires?',
                                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
                
                            if reply == QMessageBox.Yes:
                                # Activer le curseur occupé pour la suppression
                                self.set_busy_cursor(True)
                            
                                # IMPORTANT: Ne pas nettoyer les fichiers locaux ici
                                # Supprimer uniquement les PDFs non locaux
                                if self.using_local_copies and hasattr(self.local_files_manager, 'local_temp_dir'):
                                    temp_dir = self.local_files_manager.local_temp_dir
                                    pdfs_non_locaux = [pdf for pdf in pdfs_tries if not pdf.startswith(temp_dir)]
                                    if pdfs_non_locaux:
                                        self.supprimer_pdfs_temporaires(pdfs_non_locaux)
                                else:
                                    # Si pas de copies locales, supprimer tous les PDFs
                                    self.supprimer_pdfs_temporaires(pdfs_tries)
                                
                                # IMPORTANT: Restaurer le curseur normal après la suppression
                                self.set_busy_cursor(False)
                        else:
                            # Restaurer le curseur normal après échec de la méthode alternative
                            self.set_busy_cursor(False)
                        
                            QMessageBox.critical(self, "Erreur", "Impossible de fusionner les PDF.")
                        
                            # Afficher quand même le temps total d'exécution
                            if global_duration:
                                minutes = int(global_duration) // 60
                                secondes = int(global_duration) % 60
                                self.log(f"⏱️ Temps total d'exécution (échec): {minutes}min {secondes}s ({global_duration:.2f}s)")
                    else:
                        # L'utilisateur a décliné la méthode alternative
                        self.log("Fusion alternative annulée par l'utilisateur.")
        except Exception as e:
            self.log(f"❌ Erreur générale pendant la fusion: {str(e)}")
            traceback.print_exc()
        finally:
            # IMPORTANT: Toujours restaurer le curseur normal, même en cas d'erreur
            # Utiliser force=True pour s'assurer que tous les curseurs sont réinitialisés
            self.cursor_manager.set_normal(force=True)
            # Alternative: self.set_busy_cursor(False)
        
            # Afficher quand même le temps total d'exécution en cas d'erreur
            if hasattr(self, 'global_start_time') and self.global_start_time is not None:
                global_duration = time.time() - self.global_start_time
                minutes = int(global_duration) // 60
                secondes = int(global_duration) % 60
                self.log(f"⏱️ Temps total d'exécution (avec erreur): {minutes}min {secondes}s ({global_duration:.2f}s)")
          
    def fusionner_avec_pikepdf_par_lots(self, pdf_files, output_path):
        """Fusion par lots optimisée avec pikepdf pour grands volumes"""
        try:
            self.log("[FUSION] Utilisation de pikepdf par lots (mode optimisé)...")
        
            # Paramètres optimisés pour les grands volumes
            batch_size = 30  # Taille de lot optimisée
            total = len(pdf_files)
        
            # Créer des lots de taille optimale
            batches = [pdf_files[i:i+batch_size] for i in range(0, total, batch_size)]
            self.log(f"[FUSION] Traitement en {len(batches)} lots...")
        
            # Créer un répertoire temporaire pour les lots intermédiaires
            temp_dir = tempfile.mkdtemp()
            temp_files = []
        
            # OPTIMISATION: Traitement parallèle des lots
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(batches))) as executor:
                # Fonction pour traiter un lot
                def process_batch(batch_idx, batch_pdfs):
                    fd, temp_path = tempfile.mkstemp(suffix=f"_lot{batch_idx:03d}.pdf")
                    os.close(fd)
                
                    # Traiter le lot
                    pdf = pikepdf.Pdf.new()
                    success = False
                
                    for pdf_path in batch_pdfs:
                        try:
                            src = pikepdf.Pdf.open(pdf_path)
                            pdf.pages.extend(src.pages)
                            src.close()
                            success = True
                        except Exception as e:
                            self.log(f"⚠️ Erreur dans lot {batch_idx} avec {os.path.basename(pdf_path)}")
                
                    if success:
                        # Sauvegarder le lot
                        pdf.save(temp_path)
                        return (batch_idx, temp_path, True)
                    else:
                        return (batch_idx, None, False)
            
                # Soumettre tous les lots en parallèle
                future_to_batch = {executor.submit(process_batch, i, batch): i 
                                  for i, batch in enumerate(batches)}
            
                # Collecter les résultats
                for future in concurrent.futures.as_completed(future_to_batch):
                    batch_idx, temp_path, success = future.result()
                    if success and temp_path:
                        temp_files.append((batch_idx, temp_path))
                        self.log(f"[FUSION] Lot {batch_idx+1}/{len(batches)} traité")
        
            # Trier les lots par index
            temp_files.sort(key=lambda x: x[0])
            temp_paths = [temp[1] for temp in temp_files]
        
            if not temp_paths:
                self.log("❌ Aucun lot n'a pu être créé")
                return False
        
            # Fusion finale des lots intermédiaires
            self.log("[FUSION] Fusion finale des lots...")
            final_pdf = pikepdf.Pdf.new()
        
            for temp_path in temp_paths:
                try:
                    src = pikepdf.Pdf.open(temp_path)
                    final_pdf.pages.extend(src.pages)
                    src.close()
                except Exception as e:
                    self.log(f"⚠️ Erreur avec un lot intermédiaire: {str(e)}")
        
            # Sauvegarder le résultat final
            final_pdf.save(output_path)
        
            # Nettoyage des fichiers temporaires
            for _, temp_path in temp_files:
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except:
                    pass
        
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir, ignore_errors=True)
            except:
                pass
        
            return os.path.exists(output_path) and os.path.getsize(output_path) > 0
        
        except Exception as e:
            self.log(f"❌ Erreur fusion pikepdf par lots: {str(e)}")
            traceback.print_exc()
            return False

    # Version optimisée pour fusionner avec pikepdf
    def fusionner_avec_pikepdf(self, pdf_files, output_path):
        """Fusionner les PDF avec pikepdf (plus rapide et plus stable)"""
        try:
            self.log("[FUSION] Utilisation de pikepdf (mode rapide)...")
            
            # Optimisation: fusion par lots pour réduire l'utilisation mémoire
            batch_size = 50  # Taille optimale par lot
            total = len(pdf_files)
            
            if total > batch_size*2:
                # Pour grands volumes, utiliser une approche par lots
                self.log(f"[FUSION] Traitement par lots ({total} fichiers)...")
                
                # Créer un répertoire temporaire pour stocker les PDFs intermédiaires
                temp_dir = tempfile.mkdtemp()
                temp_files = []
                
                # Traiter par lots
                batches = [pdf_files[i:i+batch_size] for i in range(0, total, batch_size)]
                
                for i, batch in enumerate(batches):
                    # Nom temporaire pour le lot
                    fd, temp_path = tempfile.mkstemp(suffix=f"_lot{i:03d}.pdf")
                    os.close(fd)
                    temp_files.append(temp_path)
                    
                    # Traiter ce lot
                    pdf = pikepdf.Pdf.new()
                    
                    for pdf_path in batch:
                        try:
                            src = pikepdf.Pdf.open(pdf_path)
                            pdf.pages.extend(src.pages)
                            src.close()
                        except Exception as e:
                            self.log(f"⚠️ Erreur avec {pdf_path}: {str(e)}")
                    
                    # Sauvegarder le lot
                    pdf.save(temp_path)
                    self.log(f"[FUSION] Lot {i+1}/{len(batches)} traité ({len(batch)} fichiers)")
                    
                    # Libérer la mémoire explicitement
                    del pdf
                    import gc
                    gc.collect()
                
                # Maintenant fusionner tous les lots
                self.log("[FUSION] Fusion finale des lots...")
                final_pdf = pikepdf.Pdf.new()
                
                for temp_file in temp_files:
                    try:
                        src = pikepdf.Pdf.open(temp_file)
                        final_pdf.pages.extend(src.pages)
                        src.close()
                    except Exception as e:
                        self.log(f"⚠️ Erreur avec le lot {temp_file}: {str(e)}")
                
                # Sauvegarder le résultat final
                final_pdf.save(output_path)
                
                # Nettoyer les fichiers temporaires
                for temp_file in temp_files:
                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except:
                        pass
                
                try:
                    os.rmdir(temp_dir)
                except:
                    pass
                
                self.log(f"[FUSION] Fusion par lots terminée avec succès")
                return True
            
            else:
                # Pour petits volumes, approche directe optimisée
                pdf = pikepdf.Pdf.new()
                count = 0
                
                for file_path in pdf_files:
                    try:
                        # Ouvrir le PDF source
                        src = pikepdf.Pdf.open(file_path)
                        
                        # Copier toutes les pages
                        pdf.pages.extend(src.pages)
                        
                        # Fermer le fichier source pour libérer la mémoire
                        src.close()
                        
                        count += 1
                        
                        # Log tous les 20 fichiers pour ne pas surcharger l'UI
                        if count % 20 == 0 or count == total:
                            self.log(f"[FUSION] {count}/{total} PDF traités...")
                    except Exception as e:
                        self.log(f"⚠️ Erreur avec {file_path}: {str(e)}")
                
                # Sauvegarder le résultat
                self.log(f"[FUSION] Finalisation du fichier...")
                pdf.save(output_path)
                
                self.log(f"[FUSION] {count}/{total} PDF fusionnés avec succès")
                return count > 0
                
        except Exception as e:
            self.log(f"❌ Erreur pikepdf: {str(e)}")
            traceback.print_exc()
            return False
    # Version optimisée de fusion avec PyPDF2
    def fusionner_avec_pypdf2(self, pdf_files, output_path):
        """Fusionner les PDF avec PyPDF2 (méthode alternative)"""
        try:
            self.log("[FUSION] Utilisation de PyPDF2...")
            
            # Rediriger stderr pour supprimer les messages d'erreur
            old_stderr = sys.stderr
            sys.stderr = NullWriter()
            
            try:
                # Optimisation: Fusion par lots pour mieux gérer la mémoire
                if len(pdf_files) > 15:
                    return self.fusionner_par_lots(pdf_files, output_path)
                
                # Fusion directe pour les petits volumes
                merger = PyPDF2.PdfMerger(strict=False)
                count = 0
                
                for pdf_path in pdf_files:
                    try:
                        with open(pdf_path, 'rb') as pdf_file:
                            merger.append(pdf_file, import_bookmarks=False)
                            count += 1
                    except Exception as e:
                        self.log(f"⚠️ Erreur avec {pdf_path}")
                
                if count == 0:
                    self.log("❌ Aucun PDF n'a pu être ajouté")
                    return False
                
                merger.write(output_path)
                merger.close()
                return True
            finally:
                # Restaurer stderr
                sys.stderr = old_stderr
        except Exception as e:
            self.log(f"❌ Erreur PyPDF2: {str(e)}")
            traceback.print_exc()
            return False
    
    # Version optimisée de fusion par lots avec PyPDF2
    def fusionner_par_lots(self, pdf_files, output_path):
        """Fusion par lots pour les grands volumes avec PyPDF2"""
        try:
            # Rediriger stderr
            old_stderr = sys.stderr
            sys.stderr = NullWriter()
            
            try:
                self.log("[FUSION] Fusion par lots optimisée...")
                
                # Paramètres optimisés
                # Plus petit batch_size pour éviter les problèmes de mémoire
                total_files = len(pdf_files)
                batch_size = 10
                if total_files > 100:
                    batch_size = 8  # Encore plus petit pour très gros volumes
                
                batch_count = (total_files + batch_size - 1) // batch_size
                temp_files = []
                
                self.log(f"[FUSION] {batch_count} lots à traiter...")
                
                # Utiliser un thread pool pour traiter plusieurs lots en parallèle
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, batch_count)) as executor:
                    # Fonction pour traiter un lot
                    def process_batch(batch_idx, batch_pdfs):
                        try:
                            fd, temp_path = tempfile.mkstemp(suffix=f"_lot{batch_idx:03d}.pdf")
                            os.close(fd)
                            
                            batch_merger = PyPDF2.PdfMerger(strict=False)
                            success = False
                            
                            for pdf in batch_pdfs:
                                try:
                                    with open(pdf, 'rb') as f:
                                        batch_merger.append(f, import_bookmarks=False)
                                        success = True
                                except Exception:
                                    pass  # Ignorer les erreurs individuelles
                            
                            if success:
                                batch_merger.write(temp_path)
                                batch_merger.close()
                                del batch_merger
                                return (batch_idx, temp_path, True)
                            else:
                                batch_merger.close()
                                del batch_merger
                                return (batch_idx, temp_path, False)
                        except Exception as e:
                            return (batch_idx, None, False)
                    
                    # Créer les tâches pour chaque lot
                    futures = []
                    for i in range(batch_count):
                        start_idx = i * batch_size
                        end_idx = min(start_idx + batch_size, total_files)
                        batch = pdf_files[start_idx:end_idx]
                        futures.append(executor.submit(process_batch, i, batch))
                    
                    # Collecter les résultats
                    for future in concurrent.futures.as_completed(futures):
                        batch_idx, temp_path, success = future.result()
                        if success:
                            temp_files.append((batch_idx, temp_path))
                            self.log(f"✅ Lot {batch_idx+1} traité")
                        else:
                            self.log(f"❌ Lot {batch_idx+1} échoué")
                
                # Trier les lots par index pour préserver l'ordre
                temp_files.sort(key=lambda x: x[0])
                temp_paths = [temp[1] for temp in temp_files]
                
                if not temp_paths:
                    self.log("❌ Aucun lot n'a pu être créé")
                    return False
                
                # Fusionner les lots
                self.log("[FUSION] Fusion finale des lots...")
                final_merger = PyPDF2.PdfMerger(strict=False)
                final_success = False
                
                for i, temp_path in enumerate(temp_paths):
                    try:
                        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                            with open(temp_path, 'rb') as f:
                                final_merger.append(f, import_bookmarks=False)
                                final_success = True
                                self.log(f"[FUSION] Lot {i+1}/{len(temp_paths)} intégré")
                        else:
                            self.log(f"[FUSION] Lot {i+1} vide, ignoré")
                    except Exception as e:
                        self.log(f"[FUSION] Erreur avec lot {i+1}: {str(e)}")
                
                if not final_success:
                    self.log("❌ Fusion finale échouée")
                    return False
                
                final_merger.write(output_path)
                final_merger.close()
                
                # Nettoyage explicite pour libérer la mémoire
                del final_merger
                import gc
                gc.collect()
                
                # Nettoyage des fichiers temporaires
                for _, temp_path in temp_files:
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except:
                        pass
                
                return os.path.exists(output_path) and os.path.getsize(output_path) > 0
            finally:
                # Restaurer stderr
                sys.stderr = old_stderr
        except Exception as e:
            self.log(f"❌ Erreur dans la fusion par lots: {str(e)}")
            traceback.print_exc()
            return False
    # Version optimisée pour supprimer les fichiers temporaires
    def supprimer_pdfs_temporaires(self, pdf_paths):
        try:
            # Afficher le nombre de fichiers à supprimer
            self.log(f"[NETTOYAGE] Suppression de {len(pdf_paths)} fichiers PDF temporaires...")
        
            # Changer le curseur pour indiquer que le nettoyage est en cours
            self.set_busy_cursor(True)
        
            # Optimisation: utiliser des ensembles pour les recherches rapides
            # et ne traiter chaque fichier qu'une seule fois
            network_files = set()
            local_files = set()
        
            for pdf in pdf_paths:
                # Détecter si c'est un chemin réseau
                if self.est_chemin_reseau(pdf):
                    network_files.add(pdf)
                else:
                    local_files.add(pdf)
        
            # Compteurs pour le rapport final
            supprime_count = 0
            echec_count = 0
        
            # Traiter les fichiers locaux d'abord (plus rapide)
            if local_files:
                self.log(f"[NETTOYAGE] Suppression de {len(local_files)} fichiers locaux...")
            
                # Optimisation: utiliser un batch script pour Windows quand il y a beaucoup de fichiers
                if sys.platform == 'win32' and len(local_files) > 5:
                    batch_content = "@echo off\n"
                    for pdf in local_files:
                        batch_content += f'del /F /Q "{pdf}" 2>nul\n'
                
                    fd, batch_path = tempfile.mkstemp(suffix=".bat")
                    os.close(fd)
                    with open(batch_path, 'w', encoding='utf-8') as f:
                        f.write(batch_content)
                
                    # Exécuter le batch et attendre qu'il se termine
                    try:
                        subprocess.run(batch_path, shell=True, check=False, timeout=30)
                    except subprocess.TimeoutExpired:
                        self.log("⚠️ Délai d'attente dépassé pour le nettoyage des fichiers locaux")
                
                    try:
                        os.unlink(batch_path)
                    except:
                        pass
                else:
                    # Utiliser ThreadPoolExecutor pour les systèmes non-Windows ou petits ensembles de fichiers
                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                        futures = {executor.submit(os.remove, pdf): pdf for pdf in local_files}
                        for future in concurrent.futures.as_completed(futures):
                            pdf = futures[future]
                            try:
                                future.result()  # Vérifie le résultat et déclenche des exceptions s'il y en a
                                supprime_count += 1
                            except Exception:
                                echec_count += 1
        
            # Traiter les fichiers réseau avec plus de précaution et parallélisme limité
            if network_files:
                self.log(f"[NETTOYAGE] Suppression de {len(network_files)} fichiers réseau...")
            
                # Fonction pour supprimer un fichier réseau avec retry
                def supprimer_reseau_safe(pdf):
                    try:
                        def supprimer():
                            os.remove(pdf)
                        operation_avec_retry(supprimer, max_tentatives=2, delai_initial=1)
                        return True
                    except Exception:
                        return False
            
                # Utiliser un ThreadPool avec un nombre de workers limité pour éviter la surcharge réseau
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    # Diviser les fichiers en lots pour éviter de surcharger le réseau
                    network_files_list = list(network_files)
                    batch_size = 20
                    batches = [network_files_list[i:i+batch_size] for i in range(0, len(network_files_list), batch_size)]
                
                    # Traiter chaque lot
                    for batch in batches:
                        # Soumettre les tâches du lot
                        futures = {executor.submit(supprimer_reseau_safe, pdf): pdf for pdf in batch}
                    
                        # Attendre les résultats
                        for future in concurrent.futures.as_completed(futures):
                            pdf = futures[future]
                            try:
                                result = future.result()
                                if result:
                                    supprime_count += 1
                                else:
                                    echec_count += 1
                            except Exception:
                                echec_count += 1
                    
                        # Petit délai entre les lots
                        time.sleep(0.1)
        
            # Vérifier quels fichiers ont été réellement supprimés pour mettre à jour les compteurs
            # Optimisation: vérifier par lots pour éviter les accès disque trop fréquents
            remaining_count = 0
            batch_size = 50
            for i in range(0, len(pdf_paths), batch_size):
                batch = pdf_paths[i:min(i+batch_size, len(pdf_paths))]
                for pdf in batch:
                    if os.path.exists(pdf):
                        remaining_count += 1
        
            # Ajuster les compteurs en fonction de la vérification
            total_removed = len(pdf_paths) - remaining_count
            if total_removed != supprime_count:
                supprime_count = total_removed
                echec_count = remaining_count
                
            self.log(f"[NETTOYAGE] {supprime_count} fichiers supprimés, {echec_count} échecs")
            
            # Restaurer le curseur normal après le nettoyage
            self.set_busy_cursor(False)
        
        except Exception as e:
            self.log(f"[ERREUR] Problème lors du nettoyage: {str(e)}")
            traceback.print_exc()
            
            # Restaurer le curseur normal même en cas d'erreur
            self.set_busy_cursor(False)
    
    # Fonction pour logger les messages dans l'interface (optimisée)
    def log(self, msg):
        # Ajouter un timestamp pour les logs importants
        if "[FUSION]" in msg or "[ERREUR]" in msg or "Temps total" in msg:
            timestamp = time.strftime("%H:%M:%S")
            msg = f"[{timestamp}] {msg}"
            
        self.log_display.append(msg)
        # Auto-scroll vers le bas
        self.log_display.verticalScrollBar().setValue(self.log_display.verticalScrollBar().maximum())
        
        # Traiter les événements UI pour éviter le gel de l'interface
        QApplication.processEvents()

        # Enregistrer également dans le fichier de log
        self.file_logger.write(msg)
    
    # Fonction pour ouvrir le dernier PDF fusionné
    def open_last_pdf(self):
        """Ouvre le dernier PDF fusionné"""
        # Changer le curseur avant de vérifier le fichier
        self.set_busy_cursor(True)
    
        if self.final_pdf_path and os.path.exists(self.final_pdf_path):
            self.log(f"📄 Ouverture du PDF : {self.final_pdf_path}")
        
            # Restaurer le curseur normal avant d'ouvrir le PDF
            self.set_busy_cursor(False)
        
            if open_pdf(self.final_pdf_path):
                pass  # PDF ouvert avec succès
            else:
                self.log(f"❌ Impossible d'ouvrir le PDF : {self.final_pdf_path}")
                QMessageBox.warning(self, "Erreur", f"Impossible d'ouvrir le PDF avec le lecteur par défaut.")
        else:
            # Restaurer le curseur normal
            self.set_busy_cursor(False)
            self.log("❌ Aucun PDF fusionné disponible")
            QMessageBox.information(self, "Information", "Aucun PDF fusionné n'est disponible.")
    
    # Fonction appelée lors de la fermeture de l'application
    def closeEvent(self, event):
        """
        Méthode appelée lors de la fermeture de l'application.
        Demande une confirmation à l'utilisateur et nettoie les fichiers temporaires si confirmé.
        Optimisée pour utilisateurs sans droits administrateur.
        """
        # Réinitialiser l'état du curseur avant d'afficher le dialogue
        if hasattr(self, 'cursor_manager'):
            self.cursor_manager.reset()
        else:
            # Si le gestionnaire de curseur n'est pas disponible, assurer que le curseur est normal
            try:
                app = QApplication.instance()
                while app.overrideCursor() is not None:
                    app.restoreOverrideCursor()
            except Exception:
                pass
    
        # Demander confirmation à l'utilisateur avant de fermer l'application
        reply = QMessageBox.question(
            self, 
            'Confirmation',
            'Voulez-vous vraiment quitter l\'application?\nLes fichiers temporaires seront supprimés.',
            QMessageBox.Yes | QMessageBox.No, 
            QMessageBox.No  # Option par défaut : Non
        )
    
        if reply == QMessageBox.Yes:
            # L'utilisateur a confirmé, procéder à la fermeture
            self.log("[FERMETURE] Début du processus de fermeture...")
        
            # Changer le curseur pour indiquer le traitement
            if hasattr(self, 'cursor_manager'):
                self.cursor_manager.set_busy()
            else:
                QApplication.setOverrideCursor(Qt.WaitCursor)
        
            try:
                # Nettoyer le cache local
                if hasattr(self, 'cache_local'):
                    self.log("[FERMETURE] Nettoyage du cache local...")
                    self.cache_local.nettoyer()
        
                # Nettoyer les copies locales des DWG
                if hasattr(self, 'local_files_manager'):
                    self.log("[FERMETURE] Nettoyage des fichiers temporaires...")
                    self.local_files_manager.cleanup()
            
                    # Vérification du dossier temporaire
                    if hasattr(self.local_files_manager, 'local_temp_dir') and \
                    self.local_files_manager.local_temp_dir and \
                    os.path.exists(self.local_files_manager.local_temp_dir):
                       
                        # Suppression forcée du dossier temporaire
                        self.log(f"[FERMETURE] Suppression du dossier {self.local_files_manager.local_temp_dir}")
                        self.force_directory_deletion(self.local_files_manager.local_temp_dir)
                    
                        # Vérification finale
                        if os.path.exists(self.local_files_manager.local_temp_dir):
                            self.log("[FERMETURE] ⚠️ Le dossier temporaire sera nettoyé au prochain démarrage.")
                            self._add_to_cleanup_list(self.local_files_manager.local_temp_dir)
                        else:
                            self.log("[FERMETURE] ✅ Dossier temporaire supprimé avec succès.")
            
                # Vérifier et nettoyer d'autres dossiers temporaires
                self.cleanup_other_temp_folders()
        
                # Fermer proprement les thread pools s'ils existent
                pool_manager = ThreadPoolManager.get_instance()
                if pool_manager:
                    pool_manager.shutdown()
                
            except Exception as e:
                self.log(f"[FERMETURE] Erreur pendant le nettoyage: {str(e)}")
                import traceback
                traceback.print_exc()
            finally:
                # Restaurer le curseur normal avant de fermer
                if hasattr(self, 'cursor_manager'):
                    self.cursor_manager.set_normal(force=True)
                else:
                    try:
                        app = QApplication.instance()
                        while app.overrideCursor() is not None:
                            app.restoreOverrideCursor()
                    except Exception:
                        pass
        
            # Information finale
            self.log("[FERMETURE] Processus de fermeture terminé.")
        
            # Accepter l'événement pour permettre la fermeture
            event.accept()
        else:
            # L'utilisateur a annulé, ignorer l'événement de fermeture
            event.ignore()


    def check_previous_cleanup_list(self):
        """
        Vérifie et nettoie les dossiers qui n'ont pas pu être supprimés lors 
        des exécutions précédentes. À appeler au démarrage de l'application.
        """
        try:
            cleanup_file = os.path.join(tempfile.gettempdir(), "autocad_pdf_cleanup_list.txt")
            if not os.path.exists(cleanup_file):
                return
        
            self.log("[DÉMARRAGE] Vérification des dossiers temporaires à nettoyer...")
            with open(cleanup_file, 'r') as f:
                paths = [line.strip() for line in f.readlines() if line.strip()]
        
            if not paths:
                os.remove(cleanup_file)
                return
            
            remaining_paths = []
            removed_count = 0
        
            for path in paths:
                if os.path.exists(path):
                    # Tenter de supprimer le dossier
                    if self.force_directory_deletion(path):
                        removed_count += 1
                        self.log(f"[DÉMARRAGE] ✅ Nettoyé: {path}")
                    else:
                        remaining_paths.append(path)
                        self.log(f"[DÉMARRAGE] ⚠️ Toujours inaccessible: {path}")
        
            # Mettre à jour la liste de nettoyage
            if remaining_paths:
                with open(cleanup_file, 'w') as f:
                    for path in remaining_paths:
                        f.write(f"{path}\n")
                self.log(f"[DÉMARRAGE] {removed_count} dossiers nettoyés, {len(remaining_paths)} restants.")
            else:
                os.remove(cleanup_file)
                self.log(f"[DÉMARRAGE] Tous les dossiers ({removed_count}) ont été nettoyés avec succès.")
            
        except Exception as e:
            self.log(f"[DÉMARRAGE] Erreur lors de la vérification de la liste de nettoyage: {str(e)}")

    def force_directory_deletion(self, directory_path):
        """
        Force la suppression d'un dossier et de son contenu, 
        optimisé pour les utilisateurs sans droits administrateur
        """
        if not directory_path or not os.path.exists(directory_path):
            return False

        success = False
        self.log(f"[LOCAL] Tentative de suppression du dossier: {directory_path}")

        try:
            # Étape 1: Méthode standard avec shutil
            shutil.rmtree(directory_path, ignore_errors=True)
            if not os.path.exists(directory_path):
                success = True
                self.log("[LOCAL] Suppression standard réussie")
                return success
        
            # Étape 2: Si Windows, essayer avec rmdir (ne nécessite pas de droits admin)
            if os.name == 'nt':
                try:
                    subprocess.run(f'rmdir /S /Q "{directory_path}"', shell=True, timeout=5, check=False)
                    if not os.path.exists(directory_path):
                        success = True
                        self.log("[LOCAL] Suppression avec rmdir réussie")
                        return success
                except Exception as e:
                    self.log(f"[LOCAL] Erreur avec rmdir: {str(e)}")
        
            # Étape 3: Suppression fichier par fichier (méthode qui fonctionne même avec des droits limités)
            if os.path.exists(directory_path):
                self.log("[LOCAL] Tentative de suppression fichier par fichier...")
                try:
                    # Modification des attributs des fichiers verrouillés
                    if os.name == 'nt':
                        # Commande attrib pour enlever les attributs readonly, hidden, system
                        # Fonctionne sans droits admin pour les fichiers de l'utilisateur
                        subprocess.run(f'attrib -R -H -S "{directory_path}\\*.*" /S /D', 
                                      shell=True, timeout=5, check=False)
                
                    # Parcourir le dossier en commençant par les fichiers et sous-dossiers les plus profonds
                    for root, dirs, files in os.walk(directory_path, topdown=False):
                        # D'abord supprimer tous les fichiers
                        for file in files:
                            file_path = os.path.join(root, file)
                            try:
                                # Pour les fichiers en lecture seule, essayer de changer les permissions
                                if os.path.exists(file_path):
                                    os.chmod(file_path, 0o666)  # Permissions en lecture/écriture
                                    os.unlink(file_path)
                            except Exception:
                                pass  # Ignorer les erreurs individuelles
                    
                        # Puis essayer de supprimer les dossiers vides
                        for dir in dirs:
                            dir_path = os.path.join(root, dir)
                            try:
                                if os.path.exists(dir_path):
                                    os.rmdir(dir_path)
                            except Exception:
                                pass
                
                    # Finalement, essayer de supprimer le dossier principal
                    try:
                        if os.path.exists(directory_path):
                            os.rmdir(directory_path)
                    except Exception:
                        pass
                
                    # Vérifier si la suppression a réussi
                    if not os.path.exists(directory_path):
                        success = True
                        self.log("[LOCAL] Suppression détaillée réussie")
                        return success
                    else:
                        # Vérifier si le dossier est vide (même si on n'a pas pu le supprimer)
                        try:
                            is_empty = len(os.listdir(directory_path)) == 0
                            if is_empty:
                                self.log("[LOCAL] Dossier vidé mais non supprimé")
                                # Pourrait être considéré comme un succès partiel
                        except Exception:
                            pass
                except Exception as e:
                    self.log(f"[LOCAL] Erreur lors de la suppression détaillée: {str(e)}")
        
            # Étape 4: Informer l'utilisateur si la suppression a échoué
            if not success and os.path.exists(directory_path):
                self.log("[LOCAL] Impossible de supprimer complètement le dossier. Il sera peut-être nettoyé au redémarrage.")
                # Stocker le chemin dans les paramètres de l'application pour un nettoyage ultérieur
                self._add_to_cleanup_list(directory_path)

        except Exception as e:
            self.log(f"[LOCAL] Erreur générale lors de la suppression: {str(e)}")

        return success


if __name__ == "__main__":
    # L'application QApplication doit être créée en premier, avant tout widget
    app = QApplication(sys.argv)
    
    # Configuration optimisée du threading pour Windows
    if sys.platform == 'win32':
        try:
            import win32process
            import win32api
            # Priorité élevée pour l'application principale
            win32process.SetPriorityClass(win32process.GetCurrentProcess(), win32process.ABOVE_NORMAL_PRIORITY_CLASS)
        except ImportError:
            pass  # Win32api optionnel
    
    # Optimisation du garbage collector
    import gc
    gc.disable()  # Désactiver le GC automatique
    
    # Initialiser le pool manager après QApplication
    thread_pool = ThreadPoolManager.get_instance()
    
    # Formatage du nom d'utilisateur et affichage de l'écran de bienvenue
    formatted_username = format_username()
    splash = WelcomeSplash(formatted_username)
    splash.show()
    
    # Créer la fenêtre principale
    window = AutoCADPDFInterface()
    
    # Configurer un timer pour fermer l'écran de bienvenue après 2 secondes
    # et afficher la fenêtre principale
    timer = QTimer()
    timer.setSingleShot(True)
    timer.timeout.connect(lambda: (splash.close(), window.show()))
    timer.start(2000)  # 2000 ms = 2 secondes
    
    # Activer le GC périodique
    def gc_task():
        while True:
            time.sleep(60)  # Exécuter toutes les 60 secondes
            gc.collect()  # Collecte forcée
    
    gc_thread = threading.Thread(target=gc_task, daemon=True)
    gc_thread.start()
    
    # Lancer la boucle d'événements
    sys.exit(app.exec_())