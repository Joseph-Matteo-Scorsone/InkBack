use eframe::egui;
use egui::Color32;
use egui_plot::{Legend, Line, Plot, PlotPoints};

#[derive(Clone)]
pub struct EquityCurve {
    pub label: String,
    pub equity_data: Vec<f64>,
    pub visible: bool,
    pub color: Color32,
}

pub struct EquityPlotter {
    equity_curves: Vec<EquityCurve>,
    benchmark: Option<Vec<f64>>,
    show_benchmark: bool,
}

impl EquityPlotter {
    fn new(curves_data: Vec<(String, Vec<f64>)>, benchmark: Option<Vec<f64>>) -> Self {
        let colors = generate_colors(curves_data.len());
        let equity_curves = curves_data
            .into_iter()
            .enumerate()
            .map(|(i, (label, data))| EquityCurve {
                label,
                equity_data: data,
                visible: true,
                color: colors[i],
            })
            .collect();

        Self {
            equity_curves,
            benchmark,
            show_benchmark: true,
        }
    }
}

impl eframe::App for EquityPlotter {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::SidePanel::right("controls")
            .min_width(220.0)
            .show(ctx, |ui| {
                ui.heading("Strategy Controls");
                ui.label("Toggle visibility:");
                ui.separator();

                if self.benchmark.is_some() {
                    ui.checkbox(&mut self.show_benchmark, "Benchmark");
                    ui.separator();
                }

                egui::ScrollArea::vertical().show(ui, |ui| {
                    for curve in &mut self.equity_curves {
                        ui.horizontal(|ui| {
                            ui.colored_label(curve.color, "●");
                            ui.checkbox(&mut curve.visible, &curve.label);
                        });
                    }
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            Plot::new("equity_curves")
                .legend(Legend::default())
                .show(ui, |plot_ui| {
                    if self.show_benchmark {
                        if let Some(benchmark) = &self.benchmark {
                            let points: PlotPoints = benchmark
                                .iter()
                                .enumerate()
                                .map(|(i, &v)| [i as f64, v])
                                .collect();
                            plot_ui.line(
                                Line::new(points)
                                    .name("Benchmark")
                                    .color(Color32::WHITE)
                                    .width(2.0),
                            );
                        }
                    }

                    for curve in self.equity_curves.iter().filter(|c| c.visible) {
                        let points: PlotPoints = curve
                            .equity_data
                            .iter()
                            .enumerate()
                            .map(|(i, &v)| [i as f64, v])
                            .collect();
                        plot_ui.line(
                            Line::new(points)
                                .name(&curve.label)
                                .color(curve.color)
                                .width(1.5),
                        );
                    }
                });
        });
    }
}

fn generate_colors(count: usize) -> Vec<Color32> {
    (0..count)
        .map(|i| {
            let hue = (i as f32 * 360.0 / count.max(1) as f32) % 360.0;
            hsv_to_color32(hue, 0.85, 0.95)
        })
        .collect()
}

fn hsv_to_color32(h: f32, s: f32, v: f32) -> Color32 {
    let c = v * s;
    let x = c * (1.0 - ((h / 60.0) % 2.0 - 1.0).abs());
    let m = v - c;

    let (r, g, b) = if h < 60.0 {
        (c, x, 0.0)
    } else if h < 120.0 {
        (x, c, 0.0)
    } else if h < 180.0 {
        (0.0, c, x)
    } else if h < 240.0 {
        (0.0, x, c)
    } else if h < 300.0 {
        (x, 0.0, c)
    } else {
        (c, 0.0, x)
    };

    Color32::from_rgb(
        ((r + m) * 255.0) as u8,
        ((g + m) * 255.0) as u8,
        ((b + m) * 255.0) as u8,
    )
}

pub fn plot_equity_curves(equity_curves: Vec<(String, Vec<f64>)>, benchmark: Option<Vec<f64>>) {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("InkBack from Scorsone Enterprises")
            .with_inner_size([1200.0, 700.0]),
        ..Default::default()
    };

    if let Err(e) = eframe::run_native(
        "InkBack",
        options,
        Box::new(move |_cc| Ok(Box::new(EquityPlotter::new(equity_curves, benchmark)))),
    ) {
        eprintln!("Error running egui application: {}", e);
    }
}
