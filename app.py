from flask import Flask, render_template, request, redirect, url_for, flash, session, send_file
import json
import os

app = Flask(__name__)
app.secret_key = "energia_mother_v50_excel_importer"

CONFIG_FILE = 'dados_v50.json'
ADMIN_PASSWORD = "admin123"

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

def init_config():
    """Gera o catálogo limpo."""
    dados_base = {
        "GN_CONFIG": {
            "fixo_12m": [{"nome": f"{i}%", "b1": round(0.070615-(i*0.000706), 6), "b2": round(0.070615-(i*0.000706), 6)} for i in range(9)],
            "fixo_24m": [{"nome": "4% (Platinium)", "b1": 0.067790, "b2": 0.067790}],
            "tar": {
                "1": {"fixo": 0.0156, "en": 0.044112}, "2": {"fixo": 0.0497, "en": 0.03902}, 
                "3": {"fixo": 0.0827, "en": 0.036071}, "4": {"fixo": 0.1364, "en": 0.034789}
            }
        },
        "EE_CONFIG": {
            "BTN": {
                "potencias": {
                    "1.15": 0.1633, "2.3": 0.1873, "3.45": 0.2199, "4.6": 0.2524, 
                    "5.75": 0.2850, "6.9": 0.3175, "10.35": 0.4151, "13.8": 0.5126, 
                    "17.25": 0.6101, "20.7": 0.7077, "27.6": 0.9029, "34.5": 1.0980, "41.40": 1.2931
                },
                "EN": { "produtos": [], "tar": {"SIM": {"p1": 0.0607, "p2": 0, "p3": 0, "p4": 0}, "BIH": {"p1": 0.0835, "p2": 0.0158, "p3": 0, "p4": 0}, "TRI": {"p1": 0.2452, "p2": 0.0412, "p3": 0.0158, "p4": 0}} },
                "EV": { "produtos": [], "tar": {"SIM": {"p1": 0, "p2": 0, "p3": 0, "p4": 0}, "BIH": {"p1": 0, "p2": 0, "p3": 0, "p4": 0}, "TRI": {"p1": 0, "p2": 0, "p3": 0, "p4": 0}} }
            },
            "BTE": {
                "pot_ponta": 0.5521, "pot_contratada": 0.1272,
                "EN": { "produtos": [], "tar": {"TETRA": {"p1": 0.0397, "p2": 0.0353, "p3": 0.0285, "p4": 0.0237}} },
                "EV": { "produtos": [], "tar": {"TETRA": {"p1": 0, "p2": 0, "p3": 0, "p4": 0}} }
            }
        }
    }
    with open(CONFIG_FILE, 'w') as f: json.dump(dados_base, f, indent=4)
    return dados_base

def get_config():
    if not os.path.exists(CONFIG_FILE): cfg = init_config()
    else:
        with open(CONFIG_FILE, 'r') as f: cfg = json.load(f)
    
    try:
        for seg in ['BTN', 'BTE']:
            for ee in ['EN', 'EV']:
                if 'produtos' in cfg['EE_CONFIG'][seg][ee]:
                    cfg['EE_CONFIG'][seg][ee]['produtos'].sort(key=lambda x: safe_int(x.get('ordem', 999)))
    except: pass
    return cfg

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['admin_logged_in'] = True
            flash('Sessão de Administrador iniciada com sucesso!', 'success')
            return redirect(request.args.get('next') or url_for('index'))
        else:
            flash('Palavra-passe incorreta.', 'danger')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('admin_logged_in', None)
    flash('Sessão terminada.', 'info')
    return redirect(url_for('index'))

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
            flash("Não existem produtos importados neste segmento! Vá a Administração > Eletricidade e importe o seu Excel.", "danger")
            
    return render_template('calc_ele.html', config=cfg, res=res, segmento=seg, tipo_ee=t_ee)

@app.route('/config_ele', methods=['GET', 'POST'])
def config_ele():
    if not session.get('admin_logged_in'):
        flash('Acesso restrito.', 'warning')
        return redirect(url_for('login', next=request.url))

    full = get_config()
    cfg = full["EE_CONFIG"]
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'clear':
            full["EE_CONFIG"]["BTN"]["EN"]["produtos"] = []
            full["EE_CONFIG"]["BTN"]["EV"]["produtos"] = []
            full["EE_CONFIG"]["BTE"]["EN"]["produtos"] = []
            full["EE_CONFIG"]["BTE"]["EV"]["produtos"] = []
            
            with open(CONFIG_FILE, 'w') as f: json.dump(full, f, indent=4)
            flash('Base de dados de produtos apagada com sucesso! As TARs e Potências foram mantidas e estão seguras.', 'info')
            return redirect(url_for('config_ele'))

        elif action == 'import':
            csv_data = request.form.get('csv_data')
            if csv_data:
                full["EE_CONFIG"]["BTN"]["EN"]["produtos"] = []
                full["EE_CONFIG"]["BTN"]["EV"]["produtos"] = []
                full["EE_CONFIG"]["BTE"]["EN"]["produtos"] = []
                full["EE_CONFIG"]["BTE"]["EV"]["produtos"] = []
                
                linhas = csv_data.strip().split('\n')
                sucessos = 0
                
                for linha in linhas:
                    linha = linha.strip()
                    if not linha: continue
                    
                    if '\t' in linha: partes = linha.split('\t')
                    elif ';' in linha: partes = linha.split(';')
                    else: partes = linha.split(',')
                    
                    partes = [p.strip() for p in partes]
                    
                    seg = None; ee = None; nome = ""; precos = []
                    
                    for p in partes:
                        p_up = p.upper().replace('"', '')
                        p_limpo = p.replace('"', '').strip()
                        
                        if p_up in ['BTN', 'BTE'] and not seg: seg = p_up
                        elif p_up in ['EN', 'EV'] and not ee: ee = p_up
                        elif seg and ee and not nome and not p_limpo.replace(',','').replace('.','').replace('-','').isdigit():
                            if len(p_limpo) > 2: nome = p_limpo
                        elif nome:
                            try: precos.append(float(p_limpo.replace(',', '.')))
                            except: pass
                                
                    if seg and ee and nome:
                        p1 = precos[0] if len(precos) > 0 else 0
                        p2 = precos[1] if len(precos) > 1 else 0
                        p3 = precos[2] if len(precos) > 2 else 0
                        p4 = precos[3] if len(precos) > 3 else 0
                        
                        tipo = "SIM"
                        nome_lower = nome.lower()
                        
                        if seg == 'BTE':
                            tipo = "TETRA"
                        else:
                            if 'bihorario' in nome_lower or 'bih' in nome_lower:
                                tipo = "BIH"
                                p3, p2, p1 = p2, p1, 0
                            elif 'trihorario' in nome_lower or 'tri' in nome_lower:
                                tipo = "TRI"
                            else:
                                tipo = "SIM"
                                
                        prod = {
                            "ordem": len(full["EE_CONFIG"][seg][ee]["produtos"]) + 1,
                            "nome": nome, "tipo": tipo, "p1": p1, "p2": p2, "p3": p3, "p4": p4
                        }
                        full["EE_CONFIG"][seg][ee]["produtos"].append(prod)
                        sucessos += 1
                
                with open(CONFIG_FILE, 'w') as f: json.dump(full, f, indent=4)
                flash(f'Fantástico! Importados {sucessos} Produtos do Excel com sucesso.', 'success')
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

            cfg['BTN']['EN']['tar']['SIM']['p1'] = safe_float(request.form.get("tar_btn_SIM_p1"))
            cfg['BTN']['EN']['tar']['BIH']['p1'] = safe_float(request.form.get("tar_btn_BIH_p1"))
            cfg['BTN']['EN']['tar']['BIH']['p2'] = safe_float(request.form.get("tar_btn_BIH_p2"))
            cfg['BTN']['EN']['tar']['TRI']['p1'] = safe_float(request.form.get("tar_btn_TRI_p1"))
            cfg['BTN']['EN']['tar']['TRI']['p2'] = safe_float(request.form.get("tar_btn_TRI_p2"))
            cfg['BTN']['EN']['tar']['TRI']['p3'] = safe_float(request.form.get("tar_btn_TRI_p3"))
            
            cfg['BTE']['EN']['tar']['TETRA']['p1'] = safe_float(request.form.get("tar_bte_p1"))
            cfg['BTE']['EN']['tar']['TETRA']['p2'] = safe_float(request.form.get("tar_bte_p2"))
            cfg['BTE']['EN']['tar']['TETRA']['p3'] = safe_float(request.form.get("tar_bte_p3"))
            cfg['BTE']['EN']['tar']['TETRA']['p4'] = safe_float(request.form.get("tar_bte_p4"))

            for k_seg in ["BTN", "BTE"]:
                for k_ee in ["EN", "EV"]: full["EE_CONFIG"][k_seg][k_ee]['produtos'].sort(key=lambda x: safe_int(x.get('ordem', 999)))

            with open(CONFIG_FILE, 'w') as f: json.dump(full, f, indent=4)
            flash('Configurações alteradas guardadas com sucesso!', 'success')
            return redirect(url_for('config_ele'))
    
    return render_template('config_ele.html', config=cfg)

@app.route('/config_gas', methods=['GET', 'POST'])
def config_gas():
    if not session.get('admin_logged_in'): return redirect(url_for('login', next=request.url))
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
        with open(CONFIG_FILE, 'w') as f: json.dump(full, f, indent=4)
        flash('Gás atualizado!', 'success')
        return redirect(url_for('config_gas'))
    return render_template('config_gas.html', config=cfg)

# NOVA ROTA DE DOWNLOAD DO FICHEIRO CSV
@app.route('/download_template')
def download_template():
    if not session.get('admin_logged_in'):
        flash('Acesso restrito.', 'warning')
        return redirect(url_for('login'))
    
    # ATENÇÃO: Garante que o ficheiro com este nome exato está na mesma pasta do app.py
    try:
        return send_file('energia.csv', as_attachment=True)
    except Exception as e:
        flash('Ficheiro de exemplo não encontrado no servidor. Certifique-se de que se chama "energia.csv" e está na pasta principal.', 'danger')
        return redirect(url_for('config_ele'))

if __name__ == '__main__':
    init_config()
    app.run(port=5000, debug=True)