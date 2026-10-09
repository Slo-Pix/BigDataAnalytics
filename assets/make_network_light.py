"""Transparent network graphics for the white slide theme (cyan hubs, slate peers).

Usage: python make_network_light.py   (needs cairosvg)
Outputs into light/ : corner_tr, corner_bl, band_bottom, full_faint, swarm_single
"""
import math
import os
import random

CYAN, PEER = "#06B6D4", "#64748B"
os.makedirs("light", exist_ok=True)


def smooth(t):
    t = max(0.0, min(1.0, t))
    return t * t * (3 - 2 * t)


def make_graph(w, h, n_hubs, peers=(9, 14), spread=(50, 170), region=None, min_gap=260, seed=1):
    """Place swarms (hub + peers) inside region=(x0, y0, x1, y1); return nodes, edges."""
    rnd = random.Random(seed)
    x0, y0, x1, y1 = region or (0, 0, w, h)
    hubs = []
    for _ in range(2000):
        if len(hubs) == n_hubs:
            break
        p = (rnd.uniform(x0, x1), rnd.uniform(y0, y1))
        if all(math.dist(p, q) > min_gap for q in hubs):
            hubs.append(p)
    nodes, edges = [], []
    hub_ids = []
    for hx, hy in hubs:
        hub_ids.append(len(nodes))
        nodes.append([hx, hy, 8, CYAN, True])
        for _ in range(rnd.randint(*peers)):
            a, r = rnd.uniform(0, 2 * math.pi), rnd.uniform(*spread)
            x, y = hx + r * math.cos(a), hy + r * 0.85 * math.sin(a)
            seeder = rnd.random() < 0.2
            nodes.append([x, y, 5 if seeder else rnd.uniform(2.5, 4.5), CYAN if seeder else PEER, False])
            edges.append((hub_ids[-1], len(nodes) - 1, False))
    for i, a in enumerate(nodes):
        near = sorted((math.dist(a[:2], b[:2]), j) for j, b in enumerate(nodes) if j != i)
        for d, j in near[:2]:
            if d < 130 and rnd.random() < 0.55:
                edges.append((i, j, False))
    # dashed links from each hub to its nearest other hub (cross-swarm / DHT)
    for i in hub_ids:
        others = sorted((math.dist(nodes[i][:2], nodes[j][:2]), j) for j in hub_ids if j != i)
        for _, j in others[:2]:
            edges.append((i, j, True))
    return nodes, edges


def render(name, w, h, nodes, edges, fade, edge_op=0.45, node_op=0.9):
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">',
        f'<defs><radialGradient id="g"><stop offset="0" stop-color="{CYAN}" stop-opacity="0.5"/>'
        f'<stop offset="1" stop-color="{CYAN}" stop-opacity="0"/></radialGradient></defs>',
    ]
    for i, j, dashed in edges:
        (x1, y1, *_), (x2, y2, *_) = nodes[i], nodes[j]
        op = edge_op * fade((x1 + x2) / 2, (y1 + y2) / 2) * (0.7 if dashed else 1)
        if op < 0.01:
            continue
        extra = ' stroke-dasharray="5 7"' if dashed else ""
        col = CYAN if dashed else PEER
        out.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{col}" '
                   f'stroke-width="1.1" stroke-opacity="{op:.3f}"{extra}/>')
    for x, y, r, col, is_hub in nodes:
        f = fade(x, y)
        if f < 0.01:
            continue
        if is_hub:
            out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="38" fill="url(#g)" opacity="{f:.3f}"/>')
        out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" fill="{col}" fill-opacity="{node_op * f:.3f}"/>')
    out.append("</svg>")
    svg = "\n".join(out)
    with open(f"light/{name}.svg", "w") as fh:
        fh.write(svg)
    import cairosvg
    cairosvg.svg2png(bytestring=svg.encode(), write_to=f"light/{name}.png", output_width=w, output_height=h)
    print("wrote light/" + name)


W, H = 1920, 1080

# 1. Top-right corner accent for content slides (radial fade from the corner)
n, e = make_graph(W, H, 6, region=(1150, 50, 1860, 600), min_gap=210, seed=3)
render("corner_tr", W, H, n, e, lambda x, y: smooth(1 - math.dist((x, y), (W, 0)) / 1100))

# 2. Bottom-left corner accent (pairs with corner_tr on alternate slides)
n, e = make_graph(W, H, 5, region=(60, 520, 760, 1030), min_gap=210, seed=5)
render("corner_bl", W, H, n, e, lambda x, y: smooth(1 - math.dist((x, y), (0, H)) / 1050))

# 3. Bottom band (footer strip), fades out upward
BH = 260
n, e = make_graph(W, BH, 9, peers=(6, 10), spread=(40, 120), region=(0, 60, W, BH + 40), min_gap=200, seed=8)
render("band_bottom", W, BH, n, e, lambda x, y: smooth(y / BH * 1.2), edge_op=0.35, node_op=0.75)

# 4. Full-slide faint texture for section / Q&A / thank-you slides
n, e = make_graph(W, H, 14, region=(0, 0, W, H), min_gap=330, seed=11)
render("full_faint", W, H, n, e, lambda x, y: 0.35)

# 5. One swarm, full strength, as an illustration ("a torrent swarm")
S = 700
n, e = make_graph(S, S, 1, peers=(22, 22), spread=(90, 300), region=(S / 2, S / 2, S / 2 + 1, S / 2 + 1), seed=4)
n[0][2] = 14
render("swarm_single", S, S, n, e, lambda x, y: 1, edge_op=0.55, node_op=1)
