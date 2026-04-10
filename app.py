import os
import secrets
import sys
import json
import uuid
import csv
import io
import datetime

from flask import (
    Flask,
    Response,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from auth import register_access_control
from db_config import (
    DatabaseConfigError,
    clear_electricity_products_only,
    fetch_full_config,
    persist_imported_products,
    save_electricity_config,
    save_gas_config,
)
from keyvault import KeyVaultConfigurationError, apply_key_vault_secrets_to_app
from msal_auth import (
    acquire_token_by_auth_code,
    email_from_id_token_claims,
    get_authorization_url,
    get_msal_redirect_uri,
    is_email_in_gctools_admins,
)

app = Flask(__name__)
app.secret_key = "energia_mother_v50_excel_importer"

class PrefixStripMiddleware:
    def __init__(self, wsgi_app, prefix: str):
        self.wsgi_app = wsgi_app
        self.prefix = (prefix or "").strip().rstrip("/")

    def __call__(self, environ, start_response):
        if not self.prefix:
            return self.wsgi_app(environ, start_response)
        path = environ.get("PATH_INFO") or ""
        if path == self.prefix or path == self.prefix + "/":
            environ["PATH_INFO"] = "/"
            environ["SCRIPT_NAME"] = self.prefix
        elif path.startswith(self.prefix + "/"):
            environ["PATH_INFO"] = path[len(self.prefix) :] or "/"
            environ["SCRIPT_NAME"] = self.prefix
        return self.wsgi_app(environ, start_response)

_path_prefix = os.environ.get("GCTOOLS_PATH_PREFIX")
if _path_prefix:
    app.wsgi_app = PrefixStripMiddleware(app.wsgi_app, _path_prefix)

try:
    apply_key_vault_secrets_to_app(app)
except KeyVaultConfigurationError as e:
    sys.stderr.write(f"Key Vault configuration error: {e}\n")
    raise SystemExit(1) from e

if os.environ.get("GCTOOLS_BEARER_TOKEN"): app.config["GCTOOLS_BEARER_TOKEN"] = os.environ["GCTOOLS_BEARER_TOKEN"].strip()
if os.environ.get("MSAL_CLIENT_ID"): app.config["MSAL_CLIENT_ID"] = os.environ["MSAL_CLIENT_ID"].strip()
if os.environ.get("MSAL_CLIENT_SECRET"): app.config["MSAL_CLIENT_SECRET"] = os.environ["MSAL_CLIENT_SECRET"].strip()
if os.environ.get("MSAL_TENANT_ID"): app.config["MSAL_TENANT_ID"] = os.environ["MSAL_TENANT_ID"].strip()

register_access_control(app)

def safe_float(val, default=0.0):
    try:
        if val is None or val == '': return default
        return float(str(val).replace(',', '.'))
    except: return default

def safe_int(val, default=0):
    try:
        if val is None or val == '': return default
        return int(val)
    except: return default

def get_config():
    cfg = fetch_full_config()
    try:
        for seg in ['BTN', 'BTE']:
            for ee in ['EN', 'EV']:
                if 'produtos' in cfg['EE_CONFIG'][seg][ee]:
                    cfg['EE_CONFIG'][seg][ee]['produtos'].sort(key=lambda x: safe_int(x.get('ordem', 999)))
    except Exception: pass
    return cfg

def _apply_electricity_tar_from_form(cfg):
    t_en = cfg['BTN']['EN']['tar']
    v = request.form.get("tar_btn_SIM_p1")
    if v is not None and str(v).strip() != '': t_en['SIM']['p1'] = safe_float(v)
    for fk, tk in [("tar_btn_BIH_p2", "p2"), ("tar_btn_BIH_p3", "p3")]:
        v = request.form.get(fk)
        if v is not None and str(v).strip() != '': t_en['BIH'][tk] = safe_float(v)
    for fk, tk in [("tar_btn_TRI_p1", "p1"), ("tar_btn_TRI_p2", "p2"), ("tar_btn_TRI_p3", "p3")]:
        v = request.form.get(fk)
        if v is not None and str(v).strip() != '': t_en['TRI'][tk] = safe_float(v)
    tet = cfg['BTE']['EN']['tar']['TETRA']
    for i in range(1, 5):
        fk = f"tar_bte_p{i}"
        v = request.form.get(fk)
        if v is not None and str(v).strip() != '': tet[f'p{i}'] = safe_float(v)

@app.route("/login")
def login():
    if session.get("admin_logged_in"): return redirect(url_for("index"))
    state = secrets.token_urlsafe(32)
    session["msal_state"] = state
    auth_url = get_authorization_url(app, get_msal_redirect_uri(), state)
    return redirect(auth_url)

@app.route("/auth/callback")
def auth_callback():
    if request.args.get("error"): return Response("Microsoft sign-in cancelled.", status=403, mimetype="text/plain")
    state = request.args.get("state")
    if not state or state != session.get("msal_state"): return Response("Invalid state.", status=403, mimetype="text/plain")
    session.pop("msal_state", None)
    code = request.args.get("code")
    if not code: return Response("Missing code.", status=403, mimetype="text/plain")
    result = acquire_token_by_auth_code(app, code, get_msal_redirect_uri())
    email = email_from_id_token_claims(result)
    if not email: return Response("Could not read email.", status=403, mimetype="text/plain")
    if not is_email_in_gctools_admins(email): return Response("Not admin.", status=403, mimetype="text/plain")
    session["admin_logged_in"] = True
    session["admin_email"] = email
    flash("Sessão iniciada.", "success")
    return redirect(url_for("index"))

@app.route("/logout")
def logout():
    session.pop("admin_logged_in", None)
    session.pop("admin_email", None)
    flash("Sessão terminada.", "info")
    return redirect(url_for("login"))

@app.route('/')
def index(): return render_template('index.html')

@app.route('/gas', methods=['GET', 'POST'])
def calc_gas():
    cfg = get_config()["GN_CONFIG"]
    res = None
    t_fid = request.form.get('tipo_fid', 'fixo_12m')
    esc = request.form.get('escalao', '1')
    opc = request.form.get('opcao', 'b1')
    
    if request.method == 'POST':
        cons = safe_float(request.form.get('consumo'))
        dias = safe_int(request.form.get('dias', 30))
        lista_p = cfg.get(t_fid, cfg['fixo_12m'])
        p_idx = safe_int(request.form.get('prod_idx', 0))
        if p_idx >= len(lista_p): p_idx = 0
        p = lista_p[p_idx]
        tar = cfg['tar'][esc]
        nos_en_u = p[opc] + tar['en']
        nos_en_t = cons * nos_en_u
        nos_tf_u = tar['fixo']
        nos_tf_t = dias * nos_tf_u
        nos_total = nos_en_t + nos_tf_t
        cli_en_u = safe_float(request.form.get('c_en_b')) + safe_float(request.form.get('c_en_t'))
        cli_en_t = cons * cli_en_u
        cli_tf_u = safe_float(request.form.get('c_tf_b')) + safe_float(request.form.get('c_tf_t'))
        cli_tf_t = dias * cli_tf_u
        cli_total = cli_en_t + cli_tf_t
        if cons > 0 or dias > 0:
            poup_mensal = cli_total - nos_total
            res = {
                "nos_total": round(nos_total, 2), "cli_total": round(cli_total, 2),
                "poup": round(poup_mensal, 2), "poup_anual": round((poup_mensal / max(dias, 1)) * 365, 2),
                "prod_nome": p['nome'], "nos_det": {"en_u": round(nos_en_u, 6), "en_t": round(nos_en_t, 2), "tf_u": round(nos_tf_u, 4), "tf_t": round(nos_tf_t, 2)},
                "cli_det": {"en_u": round(cli_en_u, 6), "en_t": round(cli_en_t, 2), "tf_u": round(cli_tf_u, 4), "tf_t": round(cli_tf_t, 2)}
            }
    return render_template('calc_gas.html', config=cfg, res=res, tipo_fid=t_fid, esc=esc, opcao=opc)

@app.route('/eletricidade', methods=['GET', 'POST'])
def calc_ele():
    cfg = get_config()["EE_CONFIG"]
    res = None
    seg = request.form.get('segmento', 'BTN')
    t_ee = request.form.get('tipo_ee', 'EN')
    
    if request.method == 'POST':
        lista_p = cfg[seg][t_ee]['produtos']
        p_idx = safe_int(request.form.get('prod_idx', 0))
        if len(lista_p) > 0:
            if p_idx >= len(lista_p): p_idx = 0
            prod = lista_p[p_idx]
            tar = cfg[seg][t_ee]['tar'][prod['tipo']]
            dias = safe_int(request.form.get('dias', 30))
            
            nos_en_t = 0; cli_en_t = 0; det_h = []
            for i in range(1, 5):
                c = safe_float(request.form.get(f'cons_p{i}'))
                if c > 0 or i == 1:
                    nu = prod[f'p{i}'] if t_ee == 'EV' else prod[f'p{i}'] + tar[f'p{i}']
                    nt = c * nu
                    nos_en_t += nt
                    cu = safe_float(request.form.get(f'c_en_b_p{i}')) + safe_float(request.form.get(f'c_en_t_p{i}'))
                    ct = c * cu
                    cli_en_t += ct
                    det_h.append({"p": f"P{i}", "c": c, "nu_base": round(prod[f'p{i}'], 6), "nu_tar": round(tar[f'p{i}'], 6) if t_ee == 'EN' else 0, "nu": round(nu, 6), "nt": round(nt, 2), "cu": round(cu, 6), "ct": round(ct, 2)})

            if seg == 'BTN':
                pot_k = request.form.get('pot_btn', '1.15')
                nos_p_u = cfg['BTN']['potencias'].get(pot_k, 0.0573)
                cli_p_u = safe_float(request.form.get('c_pot_b')) + safe_float(request.form.get('c_pot_t'))
                nos_p_t = dias * nos_p_u
                cli_p_t = dias * cli_p_u
                p_label = f"{pot_k} kVA"
            else:
                pk, ck = safe_float(request.form.get('bte_ponta_kw')), safe_float(request.form.get('bte_cont_kw'))
                nos_p_u = (pk * cfg['BTE']['pot_ponta']) + (ck * cfg['BTE']['pot_contratada'])
                cli_p_u = (pk * (safe_float(request.form.get('c_ponta_b')) + safe_float(request.form.get('c_ponta_t'))) + ck * (safe_float(request.form.get('c_cont_b')) + safe_float(request.form.get('c_cont_t'))))
                nos_p_t = dias * nos_p_u
                cli_p_t = dias * cli_p_u
                p_label = f"BTE ({pk}kW / {ck}kW)"

            nos_total = nos_en_t + nos_p_t
            cli_total = cli_en_t + cli_p_t

            if cli_total > 0 or nos_total > 0:
                poup_mensal = cli_total - nos_total
                res = {"nos": round(nos_total, 2), "cli": round(cli_total, 2), "poup": round(poup_mensal, 2), "poup_anual": round((poup_mensal / max(dias, 1)) * 365, 2), "prod": prod['nome'], "det": det_h, "nos_p": round(nos_p_t, 2), "cli_p": round(cli_p_t, 2), "pl": p_label, "dias": dias, "nos_p_u": round(nos_p_u, 4), "cli_p_u": round(cli_p_u, 4)}
        else:
            flash("Não existem produtos importados neste segmento!", "danger")
            
    return render_template('calc_ele.html', config=cfg, res=res, segmento=seg, tipo_ee=t_ee)

@app.route('/api/sniper/ele', methods=['POST'])
def sniper_ele():
    cfg = get_config()["EE_CONFIG"]
    seg = request.form.get('segmento', 'BTN')
    dias = safe_int(request.form.get('dias', 30))

    cli_en_t = sum(safe_float(request.form.get(f'cons_p{i}')) * (safe_float(request.form.get(f'c_en_b_p{i}')) + safe_float(request.form.get(f'c_en_t_p{i}'))) for i in range(1, 5))

    if seg == 'BTN':
        pot_k = request.form.get('pot_btn', '1.15')
        cli_p_t = dias * (safe_float(request.form.get('c_pot_b')) + safe_float(request.form.get('c_pot_t')))
        nos_p_t = dias * cfg['BTN']['potencias'].get(pot_k, 0.0573)
    else:
        pk, ck = safe_float(request.form.get('bte_ponta_kw')), safe_float(request.form.get('bte_cont_kw'))
        cli_p_t = dias * (pk * (safe_float(request.form.get('c_ponta_b')) + safe_float(request.form.get('c_ponta_t'))) + ck * (safe_float(request.form.get('c_cont_b')) + safe_float(request.form.get('c_cont_t'))))
        nos_p_t = dias * ((pk * cfg['BTE']['pot_ponta']) + (ck * cfg['BTE']['pot_contratada']))

    cli_total = cli_en_t + cli_p_t

    def get_top_for_type(tipo_ee):
        lista_p = cfg[seg][tipo_ee]['produtos']
        if not lista_p: return []
        resultados = []
        for idx, prod in enumerate(lista_p):
            tar = cfg[seg][tipo_ee]['tar'][prod['tipo']]
            nos_en_t = sum(safe_float(request.form.get(f'cons_p{i}')) * (prod[f'p{i}'] if tipo_ee == 'EV' else prod[f'p{i}'] + tar[f'p{i}']) for i in range(1, 5))
            nos_total = nos_en_t + nos_p_t
            resultados.append({"idx": idx, "nome": prod['nome'], "nos_total": round(nos_total, 2), "poup_anual": round(((cli_total - nos_total) / max(dias, 1)) * 365, 2), "tipo": tipo_ee})
        resultados.sort(key=lambda x: x['poup_anual'], reverse=True)
        return resultados[:2] 

    top_en = get_top_for_type('EN')
    top_ev = get_top_for_type('EV')

    veredicto = ""
    if top_en and top_ev:
        best_en = top_en[0]
        best_ev = top_ev[0]
        diff_mensal = best_ev['nos_total'] - best_en['nos_total']
        
        if diff_mensal < -0.1:
            veredicto = "🟢 Campanha Verde Ativa: A Energia Eco fica mais barata que a Normal!"
        elif abs(diff_mensal) <= 0.1:
            veredicto = "⚖️ Preços Iguais: Ofereça a Energia Verde 100% Renováveis sem custo extra!"
        else:
            veredicto = f"🌱 Opção Eco: Passar para Energia Verde fica a apenas +{round(diff_mensal, 2)}€/mês que a Normal."
    elif top_en:
        veredicto = "⚡ Apenas Energia Normal disponível no catálogo para este segmento."
    else:
        veredicto = "🌱 Apenas Energia Verde disponível no catálogo para este segmento."

    return jsonify({ 
        "cli_total": round(cli_total, 2), 
        "top_en": top_en, 
        "top_ev": top_ev, 
        "veredicto": veredicto 
    })

@app.route('/config_ele', methods=['GET', 'POST'])
def config_ele():
    full = get_config()
    cfg = full["EE_CONFIG"]
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'clear':
            clear_electricity_products_only()
            flash('Base apagada!', 'info')
            return redirect(url_for('config_ele'))
        elif action == 'save':
            for tipo_ee in ['EN', 'EV']:
                for i, p in enumerate(cfg['BTN'][tipo_ee]['produtos']):
                    p['ordem'] = safe_int(request.form.get(f"btn_{tipo_ee.lower()}_ordem_{i}", p.get('ordem', 999)))
                    p['nome'] = request.form.get(f"btn_{tipo_ee.lower()}_nome_{i}", p['nome'])
                    p['p1'] = safe_float(request.form.get(f"btn_{tipo_ee.lower()}_p1_{i}"))
                    p['p2'] = safe_float(request.form.get(f"btn_{tipo_ee.lower()}_p2_{i}"))
                    p['p3'] = safe_float(request.form.get(f"btn_{tipo_ee.lower()}_p3_{i}"))
                for i, p in enumerate(cfg['BTE'][tipo_ee]['produtos']):
                    p['ordem'] = safe_int(request.form.get(f"bte_{tipo_ee.lower()}_ordem_{i}", p.get('ordem', 999)))
                    p['nome'] = request.form.get(f"bte_{tipo_ee.lower()}_nome_{i}", p['nome'])
                    p['p1'] = safe_float(request.form.get(f"bte_{tipo_ee.lower()}_p1_{i}"))
                    p['p2'] = safe_float(request.form.get(f"bte_{tipo_ee.lower()}_p2_{i}"))
                    p['p3'] = safe_float(request.form.get(f"bte_{tipo_ee.lower()}_p3_{i}"))
                    p['p4'] = safe_float(request.form.get(f"bte_{tipo_ee.lower()}_p4_{i}"))
            for k in cfg['BTN']['potencias']: cfg['BTN']['potencias'][k] = safe_float(request.form.get(f"pot_btn_{k}"))
            cfg['BTE']['pot_ponta'] = safe_float(request.form.get("bte_pot_ponta"))
            cfg['BTE']['pot_contratada'] = safe_float(request.form.get("bte_pot_cont"))
            _apply_electricity_tar_from_form(cfg)
            save_electricity_config(full["EE_CONFIG"])
            flash('Guardado com sucesso!', 'success')
            return redirect(url_for('config_ele'))
    return render_template('config_ele.html', config=cfg)

@app.route('/config_gas', methods=['GET', 'POST'])
def config_gas():
    full = get_config()
    cfg = full["GN_CONFIG"]
    if request.method == 'POST':
        for i, p in enumerate(cfg['fixo_12m']):
            p['b1'] = safe_float(request.form.get(f"f12_b1_{i}"))
            p['b2'] = safe_float(request.form.get(f"f12_b2_{i}"))
        cfg['fixo_24m'][0]['b1'] = safe_float(request.form.get("f24_b1_0"))
        cfg['fixo_24m'][0]['b2'] = safe_float(request.form.get("f24_b2_0"))
        for k in cfg['tar']:
            cfg['tar'][k]['fixo'] = safe_float(request.form.get(f"tar_f_{k}"))
            cfg['tar'][k]['en'] = safe_float(request.form.get(f"tar_e_{k}"))
        save_gas_config(full["GN_CONFIG"])
        flash('Gás atualizado!', 'success')
        return redirect(url_for('config_gas'))
    return render_template('config_gas.html', config=cfg)

@app.route('/download_template')
def download_template():
    try: return send_file('energia.csv', as_attachment=True)
    except Exception as e:
        flash('Ficheiro de exemplo não encontrado.', 'danger')
        return redirect(url_for('config_ele'))

# ==========================================
# NOTAS E CRM
# ==========================================
NOTES_FILE = "notes.json"

def load_notes():
    if not os.path.exists(NOTES_FILE): return {}
    with open(NOTES_FILE, "r", encoding="utf-8") as f: return json.load(f)

def save_notes(data):
    with open(NOTES_FILE, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)

@app.route('/api/notes', methods=['GET'])
def get_notes():
    token = request.args.get('token') or session.get("admin_logged_in")
    return jsonify({"notes": load_notes().get(str(token), [])}) if token else jsonify({"error": "Acesso negado."}), 403

@app.route('/api/notes', methods=['POST'])
def save_note():
    token = request.args.get('token') or session.get("admin_logged_in")
    if not token: return jsonify({"error": "Acesso negado."}), 403
    req = request.json or {}
    data = load_notes()
    user_notes = data.get(str(token), [])
    
    note_id = req.get('id')
    f_date = req.get('followup_date', '')
    f_time = req.get('followup_time', '')
    scenario = req.get('scenario')
    
    now_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    if note_id:
        for n in user_notes:
            if n['id'] == note_id:
                n['last_updated'] = now_str 
                if scenario:
                    if 'scenarios' not in n: n['scenarios'] = []
                    if len(n['scenarios']) >= 3: n['scenarios'].pop(0)
                    n['scenarios'].append(scenario)
                else:
                    # 🚀 LÓGICA DE CADEADOS: Só atualiza se o Admin não bloqueou o campo
                    locks = n.get('locked_fields', {})
                    if not locks.get('title'): n['title'] = req.get('title', n['title'])
                    if not locks.get('subtitle'): n['subtitle'] = req.get('subtitle', n['subtitle'])
                    if not locks.get('nipc'): n['nipc'] = req.get('nipc', n['nipc'])
                    if not locks.get('phone'): n['phone'] = req.get('phone', n['phone'])
                    if not locks.get('desc'): n['desc'] = req.get('desc', n['desc'])
                    if not locks.get('status'): n['status'] = req.get('status', n['status'])
                    
                    n['followup_date'] = f_date
                    n['followup_time'] = f_time
                break
    else:
        if len(user_notes) >= 30: return jsonify({"error": "Limite máximo"}), 400
        note_id = str(uuid.uuid4())
        user_notes.append({
            "id": note_id, "title": req.get('title', 'Nova Lead'), "subtitle": req.get('subtitle', ''), 
            "nipc": req.get('nipc', ''), "phone": req.get('phone', ''), 
            "desc": req.get('desc', ''), "status": req.get('status', 'lead'),
            "followup_date": f_date, "followup_time": f_time,
            "scenarios": [],
            "archived": False,
            "locked_fields": {}, # 🚀 Inicializa cadeados vazios
            "last_updated": now_str,
            "created_at": now_str
        })
    
    data[str(token)] = user_notes
    save_notes(data)
    return jsonify({"success": True, "id": note_id})

@app.route('/api/notes/<note_id>', methods=['DELETE'])
def delete_note(note_id):
    token = request.args.get('token') or session.get("admin_logged_in")
    data = load_notes()
    
    for n in data.get(str(token), []):
        if n['id'] == str(note_id):
            n['archived'] = True
            n['last_updated'] = datetime.datetime.now().strftime("%Y-%m-%d")
            break
            
    save_notes(data)
    return jsonify({"success": True})

# 🚀 NOVA ROTA DE GESTÃO ADMIN: Mover, Bloquear, Editar e Apagar Cenários
@app.route('/api/admin/manage_note', methods=['POST'])
def admin_manage_note():
    if not session.get("admin_logged_in"): 
        return jsonify({"error": "Acesso negado."}), 403
    
    req = request.json
    source_token = req.get('source_token')
    note_id = req.get('note_id')
    action = req.get('action') # 'update', 'move', 'delete_scenario'
    
    data = load_notes()
    
    if source_token not in data:
        return jsonify({"error": "Operador não encontrado."}), 404

    note_to_action = None
    note_index = -1
    for i, n in enumerate(data[source_token]):
        if n['id'] == note_id:
            note_to_action = n
            note_index = i
            break
    
    if not note_to_action:
        return jsonify({"error": "Nota não encontrada."}), 404

    if action == 'update':
        fields = ['title', 'subtitle', 'nipc', 'phone', 'desc', 'status']
        for f in fields:
            if f in req: note_to_action[f] = req[f]
        
        note_to_action['archived'] = req.get('archived', note_to_action.get('archived', False))
        note_to_action['locked_fields'] = req.get('locked_fields', note_to_action.get('locked_fields', {}))
        note_to_action['last_updated'] = datetime.datetime.now().strftime("%Y-%m-%d")
        
    elif action == 'move':
        target_token = req.get('target_token')
        if not target_token: return jsonify({"error": "Token de destino em falta."}), 400
        
        # Move de um token para outro
        note_to_move = data[source_token].pop(note_index)
        note_to_move['last_updated'] = datetime.datetime.now().strftime("%Y-%m-%d")
        
        if target_token not in data: data[target_token] = []
        data[target_token].append(note_to_move)
        
    # LÓGICA NOVA PARA APAGAR CENÁRIOS INDIVIDUAIS
    elif action == 'delete_scenario':
        scenario_idx = req.get('scenario_index')
        if scenario_idx is not None and 0 <= scenario_idx < len(note_to_action.get('scenarios', [])):
            note_to_action['scenarios'].pop(scenario_idx)
            note_to_action['last_updated'] = datetime.datetime.now().strftime("%Y-%m-%d")
        else:
            return jsonify({"error": "Índice de cenário inválido."}), 400

    save_notes(data)
    return jsonify({"success": True})

@app.route('/admin/notas')
def admin_notas():
    if not session.get("admin_logged_in"): return redirect(url_for("login"))
    today = datetime.datetime.now() 
    all_notes = load_notes()
    
    # ==========================================
    # 🚀 MOTOR DE ANALYTICS / BI DO ADMIN
    # ==========================================
    lead_velocity = {}
    scenario_counts = {}
    forgotten_leads = []
    
    for token, notes in all_notes.items():
        velocity_sum = 0
        closed_count = 0
        
        for n in notes:
            created_str = n.get('created_at', n.get('last_updated', today.strftime("%Y-%m-%d")))
            last_upd_str = n.get('last_updated', created_str)
            
            if n.get('status') == 'fechado':
                try:
                    d_created = datetime.datetime.strptime(created_str, "%Y-%m-%d")
                    d_updated = datetime.datetime.strptime(last_upd_str, "%Y-%m-%d")
                    days = (d_updated - d_created).days
                    velocity_sum += max(days, 0)
                    closed_count += 1
                except: pass
            
            for sc in n.get('scenarios', []):
                p_name = sc.get('name', 'Desconhecido')
                scenario_counts[p_name] = scenario_counts.get(p_name, 0) + 1
            
            if not n.get('archived') and n.get('status') != 'fechado':
                try:
                    d_updated = datetime.datetime.strptime(last_upd_str, "%Y-%m-%d")
                    diff_days = (today - d_updated).days
                    if diff_days >= 2:
                        forgotten_leads.append({
                            "token": token,
                            "title": n.get('title', 'Sem Título'),
                            "status": n.get('status', 'lead'),
                            "days_idle": diff_days
                        })
                except: pass
                
        if closed_count > 0:
            lead_velocity[token] = round(velocity_sum / closed_count, 1)
            
    top_sniper = sorted(scenario_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    forgotten_leads = sorted(forgotten_leads, key=lambda x: x['days_idle'], reverse=True)
    
    analytics = {
        "velocity": lead_velocity,
        "top_sniper": top_sniper,
        "forgotten": forgotten_leads
    }

    return render_template('admin_notas.html', all_notes=all_notes, today=today, datetime=datetime, analytics=analytics)

@app.route('/admin/export_leads')
def export_leads():
    if not session.get("admin_logged_in"): return redirect(url_for("login"))
    
    output = io.StringIO()
    # Adicionadas colunas para Produto e Poupança
    writer = csv.writer(output, delimiter=';') 
    writer.writerow([
        'Operador', 'Estado', 'Titulo', 'Subtitulo', 'NIPC', 'Telefone', 
        'Data Ligar', 'Hora Ligar', 'Descricao', 'Arquivado', 
        'Produto Simulado', 'Poupanca Anual', 'Data Criacao', 'Ultima Atualizacao'
    ])
    
    for token, notes in load_notes().items():
        for n in notes: 
            arq_status = 'Sim' if n.get('archived') else 'Nao'
            created = n.get('created_at', 'N/A')
            updated = n.get('last_updated', 'N/A')
            
            # Extrair dados da última simulação feita (se existir)
            scenarios = n.get('scenarios', [])
            last_prod = "N/A"
            last_poup = "0.00"
            
            if scenarios:
                # Pegamos no último cenário da lista (o mais recente)
                s = scenarios[-1]
                last_prod = s.get('name', 'N/A')
                last_poup = s.get('poup_anual', '0.00')

            writer.writerow([
                token, 
                n.get('status', '').upper(), 
                n.get('title', ''), 
                n.get('subtitle', ''), 
                n.get('nipc', ''), 
                n.get('phone', ''), 
                n.get('followup_date', ''), 
                n.get('followup_time', ''), 
                n.get('desc', '').replace('\n', ' | '), 
                arq_status,
                last_prod,   # Nova coluna
                last_poup,   # Nova coluna
                created, 
                updated
            ])
            
    response = Response(output.getvalue().encode('utf-8-sig'), mimetype="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=B2B_Leads_" + datetime.datetime.now().strftime("%Y-%m-%d_%Hh%Mm%Ss") + ".csv"
    return response

# ==========================================
# MOTOR DO CHAT E BROADCAST
# ==========================================
CHAT_FILE = "chat_sql_ready.json"

def load_chat():
    if not os.path.exists(CHAT_FILE): return {"messages": [], "broadcast": {}}
    try:
        with open(CHAT_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return {"messages": [], "broadcast": {}}

def save_chat(data):
    with open(CHAT_FILE, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)

@app.route('/api/broadcast', methods=['POST'])
def post_broadcast():
    if not session.get("admin_logged_in"): return jsonify({"error": "Unauthorized"}), 403
    text = (request.json or {}).get('text', '').strip()
    if not text: return jsonify({"error": "Empty"}), 400
    
    chat_data = load_chat()
    chat_data["broadcast"] = {
        "id": str(uuid.uuid4()),
        "text": text,
        "timestamp": datetime.datetime.now().strftime("%H:%M")
    }
    save_chat(chat_data)
    return jsonify({"success": True})

@app.route('/api/chat/status', methods=['GET'])
def get_chat_status():
    is_admin = session.get("admin_logged_in")
    token = request.args.get('token')
    chat_data = load_chat()
    
    current_broadcast = chat_data.get("broadcast", {})
    
    if is_admin:
        operators = {}
        total_unread = 0
        for msg in chat_data["messages"]:
            tid = msg["token_id"]
            if tid not in operators: operators[tid] = {"unread": 0, "last_time": ""}
            if not msg["is_read_admin"]:
                operators[tid]["unread"] += 1
                total_unread += 1
            operators[tid]["last_time"] = msg["timestamp"]
        return jsonify({"is_admin": True, "total_unread": total_unread, "operators": operators, "broadcast": current_broadcast})
    else:
        unread = sum(1 for msg in chat_data["messages"] if msg["token_id"] == token and not msg["is_read_op"])
        return jsonify({"is_admin": False, "total_unread": unread, "broadcast": current_broadcast})

@app.route('/api/chat', methods=['GET'])
def get_private_chat():
    is_admin = session.get("admin_logged_in")
    target = request.args.get('target') 
    
    if not target: return jsonify({"messages": []})
    
    chat_data = load_chat()
    filtered_msgs = []
    
    for msg in chat_data["messages"]:
        if msg["token_id"] == target:
            if is_admin: msg["is_read_admin"] = True
            else: msg["is_read_op"] = True
            filtered_msgs.append(msg)
            
    save_chat(chat_data)
    return jsonify({"messages": filtered_msgs})

@app.route('/api/chat', methods=['POST'])
def post_private_chat():
    is_admin = session.get("admin_logged_in")
    target = request.args.get('target')
    
    if not target: return jsonify({"error": "No target"}), 400
    text = (request.json or {}).get('text', '').strip()
    if not text: return jsonify({"error": "Empty"}), 400

    chat_data = load_chat()
    
    new_message = {
        "id": str(uuid.uuid4()),
        "token_id": target,
        "sender": "Admin" if is_admin else target,
        "text": text,
        "timestamp": datetime.datetime.now().strftime("%H:%M"),
        "is_read_admin": is_admin is not None, 
        "is_read_op": not is_admin 
    }
    
    chat_data["messages"].append(new_message)
    chat_data["messages"] = chat_data["messages"][-1000:]
    
    save_chat(chat_data)
    return jsonify({"success": True})

if __name__ == '__main__':
    with app.app_context():
        try: get_config()
        except DatabaseConfigError as e: sys.exit(1)
    app.run(port=5000, debug=True)