import torch
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv, GAE

class Encoder(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels):
        super().__init__()
        self.conv1 = GCNConv(in_channels, hidden_channels)
        self.conv2 = GCNConv(hidden_channels, hidden_channels)

    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index).relu()
        return self.conv2(x, edge_index)


def train_gae(data: Data):
    encoder = Encoder(in_channels=data.num_node_features, hidden_channels=64)
    model = GAE(encoder)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    for epoch in range(1, 51):
        model.train()
        optimizer.zero_grad()
        z = model.encode(data.x, data.edge_index)
        loss = model.recon_loss(z, data.edge_index)
        loss.backward()
        optimizer.step()
        if epoch % 10 == 0:
            print(f"Epoch {epoch}, loss {loss.item():.4f}")
    return model

if __name__ == "__main__":
    # TODO: загрузить Data из feature_engine
    pass