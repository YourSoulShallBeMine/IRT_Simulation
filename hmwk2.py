from scipy.stats import norm
from scipy.special import psi, gamma, sinc
import matplotlib.pyplot as plt
import numpy as np
import random
import csv
import math
import copy

# Please change the files name and run 3 times for 3 data set.

a0 = 10e-16
b0 = 10e-16
e0 = 1.0
f0 = 1.0


def logGamma(x):  # for data set 3, gamma(et) will out of range. Below is based on n! = Gamma(n+1)
    res = 0
    for i in range(2, int(x)):
        res += np.log(i)
    return res;


def Q2():
    # assign the file name
    fnumber = "1"  # the number of x, y, z data

    # read the data
    with open("X_set" + fnumber +".csv") as train_file:
        train_reader = csv.reader(train_file)
        X = []
        for t in train_reader:
            Xl = []
            for tt in t:
                Xl.append(float(tt))
            X.append(Xl)

    # X[N][d]
    print("Already load the training data!")

    # read the data
    with open("y_set" + fnumber +".csv") as test_file:
        test_reader = csv.reader(test_file)
        Y = []
        for t in test_reader:
            Y.append(float(t[0]))

    # Y[N]
    print("Already load the testing data!")

    # read the data
    with open("z_set" + fnumber +".csv") as test_file_2:
        test_reader = csv.reader(test_file_2)
        Z = []
        for t in test_reader:
            Z.append(float(t[0]))

    # Z[N]
    print("Already load the testing Z data!")


    # initialization
    N = len(X)
    d = len(X[0])
    mu = np.mat(np.zeros((d, 1), dtype='float64'))
    sigma = np.mat(np.diag(np.ones(d, dtype='float64')))
    at = np.mat(0.5 + np.zeros((d, 1)) + a0)
    bt = np.mat(b0 * np.ones((d, 1)))
    et = e0 + N/2
    ft = f0

    L5et = logGamma(et)

    # transfer X to a matrix for convenience
    Xmax = np.mat(np.zeros((N, d), dtype='float64'))
    for i in range(N):
        for j in range(d):
            Xmax[i, j] = X[i][j]

    # iteration
    T = 500
    RES = []
    for i in range(T):
        at = np.mat(0.5 + np.zeros((d, 1)) + a0)
        bt = b0 + (np.multiply(mu, mu) + sigma.diagonal().T) / 2
        # et = e0 + N/2

        ftmp = 0
        for j in range(N):
            ftmp += math.pow((Xmax[j, :] * mu)[0, 0] - Y[j], 2) + (Xmax[j, :] * sigma * Xmax[j, :].T)[0, 0]
        ft = f0 + 0.5 * ftmp

        #sigma = np.mat(np.diag(np.array(np.multiply(at, 1/bt).T[0, :])))
        sigtmp = np.zeros((d, 1))
        for j in range(d):
            sigtmp[j, 0] = at[j, 0] / bt[j, 0]
        sigma0 = np.mat(np.diag(sigtmp[:, 0].T))
        At = copy.deepcopy(sigma0)
        mu = np.mat(np.zeros((d, 1), dtype='float64'))
        sigma = np.mat(np.zeros((d, d)))
        for j in range(N):
            sigma += et/ft * (Xmax[j, :].T * Xmax[j, :])
            mu += Y[j] * Xmax[j, :].T
        sigma = (sigma + sigma0).I
        mu = sigma * (et/ft) * mu

        # Evaluate
        L1 = (e0-1) * (psi(et) - np.log(ft)) - f0 * et / ft

        L2 = 0
        for j in range(d):
            L2 += psi(at[j, 0]) - np.log(bt[j, 0])
        # L2 = 0.5 * L2 + 0.5 * (np.trace((mu * mu.T + sigma) * At))
        L2 = 0.5 * L2 - 0.5 * np.trace(np.multiply(mu * mu.T + sigma, At))

        L3 = 0
        for j in range(d):
            L3 += (a0-1) * (psi(at[j, 0]) - np.log(bt[j, 0])) - b0 * at[j, 0] / bt[j, 0]

        L4 = 0.5 * N * (psi(et) - np.log(ft))
        for j in range(N):
            L4 -= 0.5 * et / ft * (math.pow((Xmax[j, :] * mu)[0, 0] - Y[j], 2) + (Xmax[j, :] * sigma * Xmax[j, :].T)[0, 0])

        sign, logdet = np.linalg.slogdet(sigma)
        L5 = -0.5 * sign * logdet

        L6 = np.log(ft) - et + (et - 1) * psi(et) - L5et#np.log(gamma(et))

        L7 = 0
        for j in range(d):
            L7 += np.log(bt[j, 0]) - at[j, 0] + (at[j, 0] - 1) * psi(at[j, 0]) - np.log(gamma(at[j, 0]))

        L = L1 + L2 + L3 + L4 - L5 - L6 - L7
        # if (i > 220):
        #     print(i, L1, L2, L3, L4, L5, L6, L7)

        RES.append(L)
        if i%10 == 0:
            print("loop %d is done !" % i)


    # prob a
    x1 = np.linspace(1, T, T)
    plt.plot(x1, RES)
    plt.xlabel("iteration")
    plt.ylabel("L")
    plt.show()

    # show the iteration after 100
    x1 = np.linspace(1+100, T, T-100)
    plt.plot(x1, RES[100:])
    plt.xlabel("iteration")
    plt.ylabel("L")
    plt.show()

    # prob b
    x2 = np.linspace(1, d, d)
    y2 = []
    for i in range(d):
        y2.append(bt[i, 0] / at[i, 0])
    plt.plot(x2, y2)
    plt.xlabel("k")
    plt.ylabel("1 / Eq[alpha_k]")
    plt.show()

    # prob c
    print("The result of (c) is %f" % (ft/et))

    # prob d
    yh = []
    for i in range(N):
        yh.append((Xmax[i, :] * mu)[0, 0])

    plt.plot(Z, yh)
    plt.scatter(Z, Y, c ='g')
    plt.plot(Z, 10 * sinc(Z))
    plt.legend(["predict", "sinc", "real points"])
    plt.show()


Q2()